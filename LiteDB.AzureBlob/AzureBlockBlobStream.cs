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
        // configurations
        public static bool WriteDebugLogs = false;
        public static int PageSize = 1024 * 8;  // Default PageSize of LiteDB 5
        public static int Pages = 8;

        private const string MetadataLengthKey = "STREAM_LENGTH";

        readonly CloudBlobContainer Container;
        readonly ConcurrentDictionary<long, byte[]> Cache = new ConcurrentDictionary<long, byte[]>();
        const int NumberOfLocks = 111;  // should be more than enough
        readonly object[] Locks = new object[NumberOfLocks];
        readonly ConcurrentDictionary<long, byte[]> Pending = new ConcurrentDictionary<long, byte[]>();
        long LazyLength = 0;

        public AzureBlockBlobStream(string connString, string databaseName)
        {
            Container = GetContainerReference(connString, databaseName);

            for (var i = 0; i < NumberOfLocks; i++)
                Locks[i] = new object();
            if (Length != 0)
                ReadAhead(0);
        }

        private static CloudBlobContainer GetContainerReference(string connString, string databaseName)
        {
            CloudStorageAccount.TryParse(connString, out var account);
            var client = account.CreateCloudBlobClient();

            var container = client.GetContainerReference(databaseName);
            container.CreateIfNotExistsAsync().Wait();
            return container;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        private void SetLengthInternal(long newLength)
        {
            _Length = newLength;
            if (WriteDebugLogs)
                Console.WriteLine($"SetLength = {newLength / PageSize}");
            Container.Metadata[MetadataLengthKey] = newLength.ToString();
            Container.SetMetadataAsync(); // .Wait();
        }

        long? _Length = null;
        public override long Length
        {
            get
            {
                if (!_Length.HasValue)
                {
                    Container.FetchAttributesAsync().Wait();
                    if (!Container.Metadata.TryGetValue(MetadataLengthKey, out string value) || !long.TryParse(value, out long realLength))
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
            var curPage = _Length / PageSize;
            var startPage = position / PageSize;

            byte[] CachePage(int i)
            {
                var pageId = startPage + i;
                if (pageId >= curPage)
                    return null;
                var offsetStream = position + PageSize * i;
                lock (GetLock(offsetStream))
                {
                    if (Cache.TryGetValue(offsetStream, out byte[] tmp))
                        return tmp;

                    tmp = new byte[PageSize];
                    var block = Container.GetBlockBlobReference(pageId.ToString());
                    block.DownloadToByteArray(tmp, 0);
                    Cache[offsetStream] = tmp;
                    return tmp;
                }
            }

            if (bufToReturn != null)
            {
                Buffer.BlockCopy(CachePage(0), 0, bufToReturn, offset, PageSize);
                ParallelEnumerable.Range(1, Pages - 1).ForAll(i => CachePage(i));
            }
            else
            {
                ParallelEnumerable.Range(0, Pages).ForAll(i => CachePage(i));
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
                queue.AsParallel()
                    .ForAll(kv =>
                    {
                        var block = Container.GetBlockBlobReference((kv.Key / PageSize).ToString());
                        block.UploadFromByteArray(kv.Value, 0, PageSize);
                    });

                SetLengthInternal(LazyLength);
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
            var container = GetContainerReference(connString, name);
            container.DeleteIfExistsAsync().Wait();
        }

        public static void Download(string connString, string dbName, string localFile)
        {
            Console.WriteLine($"Download {dbName} to {localFile}");
            using (var s = File.OpenWrite(localFile))
            using (var page = new AzureBlockBlobStream(connString, dbName))
            {
                Debug.Assert(page.Length % PageSize == 0);
                var pageCnt = page.Length / PageSize;
                var buf = new byte[PageSize];
                for (var i = 0; i < pageCnt; i++)
                {
                    var block = page.Container.GetBlockBlobReference(i.ToString());
                    block.DownloadToByteArray(buf, 0);
                    s.Write(buf, 0, PageSize);
                }
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
                var buf = new byte[PageSize];
                using (var stream = new AzureBlockBlobStream(connString, dbName))
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
