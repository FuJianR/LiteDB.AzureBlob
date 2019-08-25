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
        // configurations
        public static bool WriteDebugLogs = false;
        public static int PageSize = 1024 * 8;  // Default PageSize of LiteDB 5
        public static int Pages = 8;
        public static string DefaultContainerName = "litedbs";
        public static long DefaultStreamSize = 1024 * 1024 * 1024; //1G
        public static PremiumPageBlobTier DefaultBlobTier = PremiumPageBlobTier.P4;

        private const string MetadataLengthKey = "STREAM_LENGTH";

        readonly CloudPageBlob Blob;
        readonly ConcurrentDictionary<long, byte[]> Cache = new ConcurrentDictionary<long, byte[]>();
        const int NumberOfLocks = 111;  // should be more than enough
        readonly object[] Locks = new object[NumberOfLocks];
        readonly ConcurrentDictionary<long, byte[]> Pending = new ConcurrentDictionary<long, byte[]>();
        readonly BlockingCollection<byte[]> Buffers = new BlockingCollection<byte[]>();
        long LazyLength = 0;

        public AzurePageBlobStream(string connString, string databaseName)
        {
            CloudPageBlob blob = GetBlobReference(connString, databaseName);
            if (!blob.ExistsAsync().Result)
            {
                if (WriteDebugLogs)
                    Console.WriteLine("Creating new page blob file " + databaseName);
                blob.CreateAsync(DefaultStreamSize).Wait();
                blob.SetPremiumBlobTierAsync(DefaultBlobTier).Wait();
            }
            Blob = blob;

            for (var i = 0; i < NumberOfLocks; i++)
                Locks[i] = new object();
            for (var i = 0; i < Math.Max(Environment.ProcessorCount * 2, 10); i++)
                Buffers.Add(new byte[PageSize * Pages]);
            if (Length != 0)
                ReadAhead(0);
        }

        private static CloudPageBlob GetBlobReference(string connString, string databaseName)
        {
            CloudStorageAccount.TryParse(connString, out var account);
            var client = account.CreateCloudBlobClient();

            var container = client.GetContainerReference(DefaultContainerName);
            container.CreateIfNotExistsAsync().Wait();

            var blob = container.GetPageBlobReference(databaseName);
            return blob;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        private void SetLengthInternal(long newLength)
        {
            _Length = newLength;
            if (WriteDebugLogs)
                Console.WriteLine($"SetLength = {newLength / PageSize}");
            Blob.Metadata[MetadataLengthKey] = newLength.ToString();
            Blob.SetMetadataAsync(); // .Wait();
        }

        long? _Length = null;
        public override long Length
        {
            get
            {
                if (!_Length.HasValue)
                {
                    Blob.FetchAttributesAsync().Wait();
                    if (!Blob.Metadata.TryGetValue(MetadataLengthKey, out string value) || !long.TryParse(value, out long realLength))
                    {
                        SetLengthInternal(0);
                        _Length = 0;
                        return 0;
                    }
                    if (realLength % PageSize != 0)
                        throw new NotImplementedException("file size is invalid!");
                    if (WriteDebugLogs)
                        Console.WriteLine($"GetLength = {realLength / PageSize}");
                    _Length = realLength;
                }
                return _Length.Value;
            }
        }

        public override long Position { get; set; }

        object GetLock(long position)
        {
            return Locks[(position / PageSize) % NumberOfLocks];
        }

        object[] GetLocks(IEnumerable<long> ps)
        {
            return ps.Select(t => (t / PageSize) % NumberOfLocks).Distinct().Select(t => Locks[t]).ToArray();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset % PageSize != 0)
                throw new NotImplementedException("invalid offset");
            if (count != PageSize)
                throw new NotImplementedException($"Read count is not {PageSize}; it is possible that you are not using LiteDB 5");

            var position = Position;

            byte[] cached;
            lock (GetLock(position))
            {
                if (!Cache.TryGetValue(position, out cached))
                    cached = null;
                else
                    Buffer.BlockCopy(cached, 0, buffer, offset, count);
                Position += count;
            }
            if (cached == null)
            {
                if (WriteDebugLogs)
                    Console.WriteLine($"Read @{position / PageSize} #{count / PageSize}");
                ReadAhead(position, buffer, offset);
                return count;
            }
            else
            {
                Task.Run(() =>
                {
                    for (var i = 1; i < Pages * 2; i++)
                    {
                        var off = position + PageSize * i;
                        if (Cache.ContainsKey(off) == false)
                        {
                            ReadAhead(off);
                            break;
                        }
                    }
                });
                return count;
            }
        }

        void ReadAhead(long position, byte[] bufToReturn = null, int offset = 0)
        {
            var buf = Buffers.Take();

            byte[] CachePage(int i)
            {
                var offsetBuf = i * PageSize;
                var offsetStream = position + offsetBuf;
                lock (GetLock(offsetStream))
                {
                    if (Cache.TryGetValue(offsetStream, out byte[] tmp))
                        return tmp;

                    tmp = new byte[PageSize];
                    Buffer.BlockCopy(buf, offsetBuf, tmp, 0, PageSize);
                    Cache[offsetStream] = tmp;
                    return tmp;
                }
            }

            if (bufToReturn != null)
            {
                var ret = Blob.DownloadRangeToByteArrayAsync(buf, 0, position, buf.Length).Result;
                Debug.Assert(ret == buf.Length);
                Buffer.BlockCopy(CachePage(0), 0, bufToReturn, offset, PageSize);

                Task.Run(() =>
                {
                    for (var i = 1; i < Pages; i++)
                        CachePage(i);
                    Buffers.Add(buf);
                });
            }
            else
            {
                Task.Run(() =>
                {
                    var ret = Blob.DownloadRangeToByteArrayAsync(buf, 0, position, buf.Length).Result;
                    Debug.Assert(ret == buf.Length);
                    for (var i = 0; i < Pages; i++)
                        CachePage(i);
                    Buffers.Add(buf);
                });
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            Debug.Assert(value % PageSize == 0);
            SetLengthInternal(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (offset != 0)
                throw new NotImplementedException("write offset is not 0; it is possible that your are not using LiteDB 5");
            if (count != PageSize)
                throw new NotImplementedException($"write count is not {PageSize}; it is possible that you are not using LiteDB 5");

            var position = Position;
            lock (GetLock(position))
            {
                if (!Cache.TryGetValue(position, out byte[] cached))
                    Cache.TryAdd(position, cached = new byte[PageSize]);
                Buffer.BlockCopy(buffer, 0, cached, 0, count);
                lock (Pending)
                {
                    Pending[position] = cached;
                }
                Position += count;
                if (Position > LazyLength)
                    LazyLength = Position;
            }
        }

        public override void Flush()
        {
            if (Pending.Count == 0)
                return;

            if (WriteDebugLogs)
                Console.WriteLine("Flush " + string.Join(",", Pending.Select(t => t.Key / PageSize)));

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
                while (true)
                {
                    var batch = new Stack<KeyValuePair<long, byte[]>>();
                    while (batch.Count < Pages && queue.Count > 0)
                    {
                        var next = queue.Peek();
                        if (batch.Count == 0 || batch.Peek().Key - PageSize == next.Key)
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
                        var task = Task.Run(() =>
                        {
                            var p = batch.Peek();
                            using (var ms = new MemoryStream(p.Value))
                            {
                                if (WriteDebugLogs)
                                    Console.WriteLine($"WriteOne @{p.Key / PageSize}");
                                Blob.WritePagesAsync(ms, p.Key, null).Wait();
                            }
                        });
                        tasks.Add(task);
                    }
                    else
                    {
                        var task = Task.Run(() =>
                        {
                            var buf = Buffers.Take();
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
                                    Debug.Assert(offsetLast + PageSize == p.Key);
                                    offsetLast = p.Key;
                                }
                                Buffer.BlockCopy(p.Value, 0, buf, offsetWithinBuf, PageSize);
                                offsetWithinBuf += PageSize;
                            }
                            if (WriteDebugLogs)
                                Console.WriteLine($"WriteBatch @{offsetStart / PageSize} #{cnt}");
                            using (var ms = new MemoryStream(buf, 0, offsetWithinBuf))
                            {
                                Blob.WritePagesAsync(ms, offsetStart, null).Wait();
                            }
                            Buffers.Add(buf);
                        });
                        tasks.Add(task);
                    }
                }

                SetLengthInternal(LazyLength);
                Task.WaitAll(tasks.ToArray());
            }
            finally
            {
                foreach (var t in locks)
                    Monitor.Exit(t);
            }
        }

        public static void DropDatabase(string connString, string name)
        {
            Console.WriteLine("Deleting " + name);
            var blob = GetBlobReference(connString, name);
            blob.DeleteIfExistsAsync().Wait();
        }

        public static void Download(string connString, string dbName, string localFile)
        {
            Console.WriteLine($"Download {dbName} to {localFile}");
            using (var s = File.OpenWrite(localFile))
            using (var page = new AzurePageBlobStream(connString, dbName))
            {
                page.Blob.DownloadRangeToStreamAsync(s, 0, page.Length).Wait();
            }
        }

        /*
        public static void Upload(string localFile, string connString, string dbName)
        {
            Console.WriteLine("Upload " + localFile);
            using (var r = File.OpenRead(localFile))
            {
                Debug.Assert(r.Length % PageSize == 0);
                Console.WriteLine(r.Length);
                var buf = new byte[1024 * 1024];
                using (var stream = new AzurePageBlobStream(connString, dbName))
                {
                    int cnt;
                    while ((cnt = r.Read(buf, 0, buf.Length)) > 0)
                        stream.Write(buf, 0, cnt);
                    stream.SetLengthInternal(r.Length);
                }
            }
        }
        */
    }
}
