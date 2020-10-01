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
    /// Use Azure Block Blob as storage backend for LiteDB 5
    /// </summary>
    public class AzureBlockBlobStream : Stream
    {
        private readonly CloudBlobContainer _container;
        private readonly ConcurrentDictionary<long, byte[]> _cache = new ConcurrentDictionary<long, byte[]>();
        private readonly object[] _locks = new object[Consts.NumberOfLocks];
        private readonly ConcurrentDictionary<long, byte[]> Pending = new ConcurrentDictionary<long, byte[]>();
        private long _lazyLength = 0;
        private long? _Length = null;

        public AzureBlockBlobStream(string connString, string databaseName)
        {
            _container = GetContainerReference(connString, databaseName);

            // Initilize locks
            for (var i = 0; i < Consts.NumberOfLocks; i++)
                _locks[i] = new object();

            if (Length != 0)
                ReadAhead(0); // Read first block immediately
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        private void SetLengthInternal(long newLength)
        {
            _Length = newLength;
            DebugLog($"SetLength = {newLength / Consts.PageSize}");
            _container.Metadata[Consts.MetadataLengthKey] = newLength.ToString();
            _container.SetMetadataAsync().SyncWait();
        }

        public override long Length
        {
            get
            {
                if (!_Length.HasValue)
                {
                    _container.FetchAttributesAsync().SyncWait();
                    if (!_container.Metadata.TryGetValue(Consts.MetadataLengthKey, out string value) || !long.TryParse(value, out long realLength))
                    {
                        SetLengthInternal(0);
                        _Length = 0;
                        return 0;
                    }
                    if (realLength % Consts.PageSize != 0)
                        throw new InvalidOperationException("File size is invalid");

                    DebugLog($"GetLength = {realLength / Consts.PageSize}");

                    _Length = realLength;
                }
                return _Length.Value;
            }
        }

        public override long Position { get; set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset != 0)
                throw new NotImplementedException("Write offset is not 0; it is possible that your are not using LiteDB 5");

            if (count != Consts.PageSize)
                throw new NotImplementedException($"Write count is not {Consts.PageSize}; it is possible that you are not using LiteDB 5");

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

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            Debug.Assert(value % Consts.PageSize == 0);
            SetLengthInternal(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (offset != 0)
                throw new InvalidOperationException("Write offset is not 0; it is possible that your are not using LiteDB 5");
            if (count != Consts.PageSize)
                throw new InvalidOperationException($"Write count is not {Consts.PageSize}; it is possible that you are not using LiteDB 5");

            var position = Position;
            lock (GetLock(position))
            {
                if (!_cache.TryGetValue(position, out byte[] cached))
                    _cache.TryAdd(position, cached = new byte[Consts.PageSize]);
                Buffer.BlockCopy(buffer, 0, cached, 0, count);
                lock (Pending)
                {
                    Pending[position] = cached;
                }
                Position += count;
                if (Position > _lazyLength)
                    _lazyLength = Position;
            }
        }

        public override void Flush()
        {
            if (Pending.Count == 0)
                return;

            DebugLog("Flush " + string.Join(",", Pending.Select(t => t.Key / Consts.PageSize)));

            Queue<KeyValuePair<long, byte[]>> queue;
            lock (Pending)
            {
                queue = new Queue<KeyValuePair<long, byte[]>>(Pending.OrderByDescending(t => t.Key));
                Pending.Clear();
            }

            var tasks = new List<Task>();
            var locks = GetLocks(queue.Select(t => t.Key));
            foreach (var m in locks)
                Monitor.Enter(m);

            try
            {
                queue.AsParallel()
                    .ForAll(kv =>
                    {
                        var block = _container.GetBlockBlobReference((kv.Key / Consts.PageSize).ToString());
                        block.UploadFromByteArray(kv.Value, 0, Consts.PageSize);
                    });

                SetLengthInternal(_lazyLength);
            }
            finally
            {
                foreach (var t in locks)
                    Monitor.Exit(t);
            }
        }

        public static void DropDatabase(string connString, string name)
        {
            DebugLog("Deleting " + name);
            var container = GetContainerReference(connString, name);
            container.DeleteIfExistsAsync().SyncWait();
        }

        public static void Download(string connString, string dbName, string localFile)
        {
            Console.WriteLine($"Download {dbName} to {localFile}");
            using (var s = File.OpenWrite(localFile))
            using (var page = new AzureBlockBlobStream(connString, dbName))
            {
                Debug.Assert(page.Length % Consts.PageSize == 0);
                var pageCnt = page.Length / Consts.PageSize;
                var buf = new byte[Consts.PageSize];
                for (var i = 0; i < pageCnt; i++)
                {
                    var block = page._container.GetBlockBlobReference(i.ToString());
                    block.DownloadToByteArray(buf, 0);
                    s.Write(buf, 0, Consts.PageSize);
                }
            }
        }

        private void ReadAhead(long position, byte[] bufToReturn = null, int offset = 0)
        {
            var curPage = _Length / Consts.PageSize;
            var startPage = position / Consts.PageSize;

            byte[] CachePage(int i)
            {
                var pageId = startPage + i;
                if (pageId >= curPage)
                    return null;
                var offsetStream = position + Consts.PageSize * i;
                lock (GetLock(offsetStream))
                {
                    if (_cache.TryGetValue(offsetStream, out byte[] tmp))
                        return tmp;

                    tmp = new byte[Consts.PageSize];
                    var block = _container.GetBlockBlobReference(pageId.ToString());
                    block.DownloadToByteArray(tmp, 0);
                    _cache[offsetStream] = tmp;
                    return tmp;
                }
            }

            if (bufToReturn != null)
            {
                Buffer.BlockCopy(CachePage(0), 0, bufToReturn, offset, Consts.PageSize);
                ParallelEnumerable.Range(1, Consts.Pages - 1).ForAll(i => CachePage(i));
            }
            else
            {
                ParallelEnumerable.Range(0, Consts.Pages).ForAll(i => CachePage(i));
            }
        }

        private object GetLock(long position)
        {
            return _locks[(position / Consts.PageSize) % Consts.NumberOfLocks];
        }

        private object[] GetLocks(IEnumerable<long> ps)
        {
            return ps.Select(t => (t / Consts.PageSize) % Consts.NumberOfLocks).Distinct().Select(t => _locks[t]).ToArray();
        }

        private static CloudBlobContainer GetContainerReference(string connString, string databaseName)
        {
            CloudStorageAccount.TryParse(connString, out var account);
            var client = account.CreateCloudBlobClient();

            var container = client.GetContainerReference(databaseName);
            container.CreateIfNotExistsAsync().SyncWait();
            return container;
        }

        [Conditional("DEBUG")]
        private static void DebugLog(string message)
            => Console.WriteLine(message);
    }
}
