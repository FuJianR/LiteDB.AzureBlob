using LiteDB;
using LiteDB.AzureBlob;
using System;
using System.IO;
using System.Linq;

namespace Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            // TestLiteDBWithAzureBlockBlob();
            TestLiteDBWithAzurePageBlob();
        }

        private static void TestLiteDBWithAzureBlockBlob()
        {
            var connectionString = "";  // <= put your Azure Premium Block Storage connection string here!!!
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new NotImplementedException("Please provide connection string");
            var databaseName = "db1";

            // write
            using (var stream = new AzureBlockBlobStream(connectionString, databaseName))
                TestWriteDatabase(stream);

            // read
            using (var stream = new AzureBlockBlobStream(connectionString, databaseName))
                TestReadDatabase(stream);

            // clean up; you can checkout the files in azure portal before deleting the file
            Console.WriteLine($"[{DateTime.Now.ToString()}] Drop database");
            AzureBlockBlobStream.DropDatabase(connectionString, databaseName);
            Console.WriteLine($"[{DateTime.Now.ToString()}] Done");
        }

        private static void TestLiteDBWithAzurePageBlob()
        {
            var connectionString = "";  // <= put your Azure Page Blob connection string here!!
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new NotImplementedException("Please provide connection string");
            var databaseName = "db1";

            // write
            using (var stream = new AzurePageBlobStream(connectionString, databaseName))
                TestWriteDatabase(stream);

            // read
            using (var stream = new AzurePageBlobStream(connectionString, databaseName))
                TestReadDatabase(stream);

            // clean up; you can checkout the files in azure portal before deleting the file
            Console.WriteLine($"[{DateTime.Now.ToString()}] Drop database");
            AzurePageBlobStream.DropDatabase(connectionString, databaseName);
            Console.WriteLine($"[{DateTime.Now.ToString()}] Done");
        }

        private static void TestWriteDatabase(Stream stream)
        {
            Console.WriteLine($"[{DateTime.Now.ToString()}] Start writing");
            using (var db = new LiteDatabase(stream))
            {
                var collection = db.GetCollection<Book>();
                ParallelEnumerable.Range(1, 1000)
                    .ForAll(i =>
                    {
                        var blog = new Book
                        {
                            Id = i,
                            Title = "fake title " + i,
                            Author = "fake author " + i,
                            Description = $"fake description {i} fake description end"
                        };
                        collection.Upsert(blog);
                    });
                db.Checkpoint(); // flush
            }
            Console.WriteLine($"[{DateTime.Now.ToString()}] Finish writing");
        }

        private static void TestReadDatabase(Stream stream)
        {
            Console.WriteLine($"[{DateTime.Now.ToString()}] Start reading");
            using (var db = new LiteDatabase(stream))
            {
                var collection = db.GetCollection<Book>();
                ParallelEnumerable.Range(1, 100)
                    .ForAll(i =>
                    {
                        var id = i * 5;
                        var blog = collection.FindById(id);
                        if (blog == null)
                            throw new NotImplementedException("Cannot find " + id);
                        else
                            Console.WriteLine($"{blog.Id}:{blog.Title}");
                    });
            }
            Console.WriteLine($"[{DateTime.Now.ToString()}] Finish reading");
        }
    }

    public class Book
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Description { get; set; }
        public string Author { get; set; }
    }
}
