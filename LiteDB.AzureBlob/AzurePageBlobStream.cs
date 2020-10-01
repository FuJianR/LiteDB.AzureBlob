using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LiteDB.AzureBlob
{
    /// <summary>
    /// Use Azure Page Blob as storage backend for LiteDB 5
    /// </summary>
    public class AzurePageBlobStream : Stream
    {
        private readonly CloudPageBlob _blog;
        private readonly ConcurrentDictionary<long, byte[]> _cache = new ConcurrentDictionary<long, byte[]>();
        private readonly object[] _locks = new object[Consts.NumberOfLocks];
        private readonly ConcurrentDictionary<long, byte[]> _pendingWrites = new ConcurrentDictionary<long, byte[]>();
        private readonly BlockingCollection<byte[]> _buffers = new BlockingCollection<byte[]>();

        private long _lazyLength = 0;
        private long? _length = null;

        public AzurePageBlobStream(string connectionString, string databaseName)
        {
            var blob = GetBlobReference(connectionString, databaseName);

            Task.Run(async () =>
            {
                if (!await blob.ExistsAsync())
                {
                    // Create database blob if it doesn't exist
                    DebugLog("Creating new page blob file " + databaseName);

                    await blob.CreateAsync(Consts.DefaultStreamSize);
                    await blob.SetPremiumBlobTierAsync(Consts.DefaultBlobTier);
                }
            }).SyncWait();

            _blog = blob;

            // Initilize locks
            for (var i = 0; i < Consts.NumberOfLocks; i++)
                _locks[i] = new object();

            // Initilize buffers
            for (var i = 0; i < Math.Max(Environment.ProcessorCount * 2, 10); i++)
                _buffers.Add(new byte[Consts.PageSize * Consts.Pages]);

            if (Length != 0)
                ReadAhead(0); // Read first block immediately
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Position { get; set; }

        public override long Length
        {
            get
            {
                if (!_length.HasValue)
                {
                    _blog.FetchAttributesAsync().SyncWait();

                    if (!_blog.Metadata.TryGetValue(Consts.MetadataLengthKey, out string value) || !long.TryParse(value, out long realLength))
                    {
                        SetLengthInternal(0);
                        _length = 0;
                        return 0;
                    }

                    if (realLength % Consts.PageSize != 0)
                        throw new InvalidOperationException("File size is invalid");

                    DebugLog($"GetLength = {realLength / Consts.PageSize}");
                    
                    _length = realLength;
                }

                return _length.Value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset % Consts.PageSize != 0)
                throw new InvalidOperationException($"Offset {offset} must be multiple of {Consts.PageSize}");

            if (count != Consts.PageSize)
                throw new InvalidOperationException($"Read count is not {Consts.PageSize}; it is possible that you are not using LiteDB 5");

            var position = Position;

            byte[] cached;

            lock (GetLock(position))
            {
                if (!_cache.TryGetValue(position, out cached))
                    cached = null;
                else
                    Buffer.BlockCopy(cached, 0, buffer, offset, count);

                Position += count;
            }

            if (cached == null)
            {
                DebugLog($"Read @{position / Consts.PageSize} #{count / Consts.PageSize}");
                ReadAhead(position, buffer, offset);
                return count;
            }
            else
            {
                Task.Run(() =>
                {
                    for (var i = 1; i < Consts.Pages * 2; i++)
                    {
                        var off = position + Consts.PageSize * i;
                        if (_cache.ContainsKey(off) == false)
                        {
                            ReadAhead(off);
                            break;
                        }
                    }
                });

                return count;
            }
        }

        public override void SetLength(long value)
        {
            Debug.Assert(value % Consts.PageSize == 0);
            SetLengthInternal(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (offset != 0)
                throw new NotImplementedException("Write offset is not 0; it is possible that your are not using LiteDB 5");

            if (count != Consts.PageSize)
                throw new NotImplementedException($"Write count is not {Consts.PageSize}; it is possible that you are not using LiteDB 5");

            var position = Position;

            lock (GetLock(position))
            {
                if (!_cache.TryGetValue(position, out byte[] cached))
                    _cache.TryAdd(position, cached = new byte[Consts.PageSize]);

                Buffer.BlockCopy(buffer, 0, cached, 0, count);

                lock (_pendingWrites)
                {
                    _pendingWrites[position] = cached;
                }

                Position += count;

                if (Position > _lazyLength)
                    _lazyLength = Position;
            }
        }

        public override void Flush()
        {
            if (_pendingWrites.Count == 0)
                return;

            DebugLog("Flush " + string.Join(",", _pendingWrites.Select(t => t.Key / Consts.PageSize)));

            Queue<KeyValuePair<long, byte[]>> queue;
            lock (_pendingWrites)
            {
                queue = new Queue<KeyValuePair<long, byte[]>>(_pendingWrites.OrderByDescending(t => t.Key));
                _pendingWrites.Clear();
            }

            var tasks = new List<Task>();
            var locks = GetLocks(queue.Select(t => t.Key));

            foreach (var m in locks)
                Monitor.Enter(m);

            try
            {
                while (true)
                {
                    var batch = new Stack<KeyValuePair<long, byte[]>>();
                    while (batch.Count < Consts.Pages && queue.Count > 0)
                    {
                        var next = queue.Peek();
                        if (batch.Count == 0 || batch.Peek().Key - Consts.PageSize == next.Key)
                        {
                            queue.Dequeue();
                            batch.Push(next);
                        }
                        else
                            break;
                    }

                    if (batch.Count == 0)
                        break;
                    else if (batch.Count == 1)
                    {
                        var task = Task.Run(async () =>
                        {
                            var p = batch.Peek();
                            using (var ms = new MemoryStream(p.Value))
                            {
                                DebugLog($"WriteOne @{p.Key / Consts.PageSize}");
                                await _blog.WritePagesAsync(ms, p.Key, null);
                            }
                        });

                        tasks.Add(task);
                    }
                    else
                    {
                        var task = Task.Run(async () =>
                        {
                            var buf = _buffers.Take();
                            long offsetStart = 0;
                            int offsetWithinBuf = 0;
                            long offsetLast = -1;
                            var cnt = batch.Count;
                            while (batch.Count > 0)
                            {
                                var p = batch.Pop();
                                if (offsetLast < 0)
                                {
                                    offsetStart = offsetLast = p.Key;
                                }
                                else
                                {
                                    Debug.Assert(offsetLast + Consts.PageSize == p.Key);
                                    offsetLast = p.Key;
                                }
                                Buffer.BlockCopy(p.Value, 0, buf, offsetWithinBuf, Consts.PageSize);
                                offsetWithinBuf += Consts.PageSize;
                            }
                            
                            DebugLog($"WriteBatch @{offsetStart / Consts.PageSize} #{cnt}");
                            
                            using (var ms = new MemoryStream(buf, 0, offsetWithinBuf))
                            {
                                await _blog.WritePagesAsync(ms, offsetStart, null);
                            }
                            _buffers.Add(buf);
                        });

                        tasks.Add(task);
                    }
                }

                SetLengthInternal(_lazyLength);
                Task.WhenAll(tasks.ToArray()).SyncWait();
            }
            finally
            {
                foreach (var t in locks)
                    Monitor.Exit(t);
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
            => throw new NotImplementedException();

        private void ReadAhead(long position, byte[] bufferToReturn = null, int offset = 0)
        {
            var buffer = _buffers.Take();

            byte[] CachePage(int i)
            {
                var offsetBuf = i * Consts.PageSize;
                var offsetStream = position + offsetBuf;
                lock (GetLock(offsetStream))
                {
                    if (_cache.TryGetValue(offsetStream, out byte[] tmp))
                        return tmp;

                    tmp = new byte[Consts.PageSize];
                    Buffer.BlockCopy(buffer, offsetBuf, tmp, 0, Consts.PageSize);
                    _cache[offsetStream] = tmp;
                    return tmp;
                }
            }

            if (bufferToReturn != null)
            {
                var downloadResult = _blog.DownloadRangeToByteArrayAsync(buffer, 0, position, buffer.Length).SyncResult();
                Debug.Assert(downloadResult == buffer.Length);
                Buffer.BlockCopy(CachePage(0), 0, bufferToReturn, offset, Consts.PageSize);

                Task.Run(() =>
                {
                    for (var i = 1; i < Consts.Pages; i++)
                        CachePage(i);

                    _buffers.Add(buffer);
                });
            }
            else
            {
                Task.Run(() =>
                {
                    var ret = _blog.DownloadRangeToByteArrayAsync(buffer, 0, position, buffer.Length).SyncResult();
                    Debug.Assert(ret == buffer.Length);
                    for (var i = 0; i < Consts.Pages; i++)
                        CachePage(i);
                    _buffers.Add(buffer);
                });
            }
        }

        public static void DropDatabase(string connString, string name)
        {
            DebugLog("Deleting " + name);
            var blob = GetBlobReference(connString, name);
            blob.DeleteIfExistsAsync().SyncWait();
        }

        private void SetLengthInternal(long newLength)
        {
            _length = newLength;
            DebugLog($"SetLength = {newLength / Consts.PageSize}");
            _blog.Metadata[Consts.MetadataLengthKey] = newLength.ToString();
            _blog.SetMetadataAsync().SyncWait();
        }

        private object GetLock(long position)
            => _locks[(position / Consts.PageSize) % Consts.NumberOfLocks];

        private object[] GetLocks(IEnumerable<long> ps)
            => ps.Select(t => (t / Consts.PageSize) % Consts.NumberOfLocks).Distinct().Select(t => _locks[t]).ToArray();

        private static CloudPageBlob GetBlobReference(string connString, string databaseName)
        {
            CloudStorageAccount.TryParse(connString, out var account);
            var client = account.CreateCloudBlobClient();

            var container = client.GetContainerReference(Consts.DefaultContainerName);
            container.CreateIfNotExistsAsync().SyncWait();

            var blob = container.GetPageBlobReference(databaseName);
            return blob;
        }

        [Conditional("DEBUG")]
        private static void DebugLog(string message)
            => Console.WriteLine(message);
    }
}