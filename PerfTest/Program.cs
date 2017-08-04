namespace Microsoft.WindowsAzure.Storage.Blob
{
	using System; 
     	using System.IO; 
     	using System.Net; 
     	using System.Threading; 
     	using System.Diagnostics; 
     	using System.IO.Compression; 
     	using System.Security.Cryptography; 
     	using Microsoft.WindowsAzure.Storage; 
     	using Microsoft.WindowsAzure.Storage.Blob; 
     	using Microsoft.WindowsAzure.Storage.RetryPolicies; 
     	using System.Threading.Tasks; 
     	using System.Text; 
     	using System.Collections.Generic; 
     	using System.Linq;
 
    class Program
    {

        public static ParallelDownload parallelDownloadObject;

	    public static CloudBlob GetBlob() 
        {
            string connectionString = "DefaultEndpointsProtocol=http;AccountName=[accountname];AccountKey=[accountkey]";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);


            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer blobContainer = blobClient.GetContainerReference("donotdelete");
            //await blobContainer.CreateIfNotExistsAsync();
            string blobName = "threehundredgig";//"onegig"; //"threehundredgig";
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference(blobName);
            return blob;
        }

        static void Main(string[] args)
        {
            OperationContext.DefaultLogLevel = LogLevel.Verbose;
            CloudBlob blob = GetBlob();
            Stopwatch time = Stopwatch.StartNew();
            int parallelCount = 8;
            long chunkSize = 4;
            ServicePointManager.DefaultConnectionLimit = 100000;
            ThreadPool.SetMinThreads(Int32.MaxValue, Int32.MaxValue);
            ThreadPool.SetMaxThreads(Int32.MaxValue, Int32.MaxValue);
            DoDownloadFileTask(blob, parallelCount /*parallel IO count*/, chunkSize * 1024 * 1024 /* range size per IO */).GetAwaiter().GetResult();
            Console.WriteLine("And, we're back in Main <-- YEAH !!!!!!!!!!!!!!!!!!!!");
            //DoParallelUploadTask().Wait();
            time.Stop();
            Console.WriteLine("Parallel I/O Count {0}.",parallelCount);
            Console.WriteLine("Download size per range {0} in MB.", chunkSize);
            Console.WriteLine("Download has been completed in {0} seconds.", time.Elapsed.TotalSeconds.ToString());
            Console.ReadLine();
            Console.ReadLine();
        }

        //private static async Task DoParallelUploadTask()
        //{
        //    string connectionString = "DefaultEndpointsProtocol=http;AccountName=xclientdev3;AccountKey=/3Hxt63L5GIDMxhTVtWEEWGgbhegrr1fDjglOQcCrbEcyUa28sKIIkA5c4x0jDuyhUWZ9f4DstQISHZiTD4LOg==";
        //    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);


        //    CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
        //    CloudBlobContainer blobContainer = blobClient.GetContainerReference("donotdelete");
        //    //await blobContainer.CreateIfNotExistsAsync();
        //    string blobName = "tengig"; //"threehundredgig";
        //    CloudBlockBlob blob = blobContainer.GetBlockBlobReference(blobName);
        //    BlobRequestOptions options = new BlobRequestOptions();
        //    options.ParallelOperationThreadCount = 20;
        //    await blob.UploadFromFileAsync("D:\\tengb.rng", null, /*null*/ options, null);
        //}

        private static async Task DoDownloadFileTask(CloudBlob blob, int parallelCount, long chunkSize)
        {
            string outputFileName = "D:\\threehundredgb.rng";//"D:\\onegb.rng"; //"D:\\threehundredgb.rng";//Path.GetTempFileName();

            try
            {
                long? offset = null;
                long? length = null;
                CloudBlob source = (CloudBlob)blob;
                ParallelDownloadSettings parallelDownloadSettings = new ParallelDownloadSettings(source, outputFileName, FileMode.Create, offset, length, parallelCount, chunkSize);
                BlobRequestOptions options = new BlobRequestOptions();
                options.DisableContentMD5Validation = true;
                options.UseTransactionalMD5 = false;
                options.ServerTimeout = TimeSpan.FromMinutes(60);
                options.MaximumExecutionTime = TimeSpan.FromDays(1.0);
                //Console.WriteLine("cranking up");
                ParallelDownload parallelDownload = ParallelDownload.Start(parallelDownloadSettings, null, null, options, CancellationToken.None);
                parallelDownloadObject = parallelDownload;
                //Console.WriteLine("About to await parallelDownload");
                await parallelDownload.Task;
                //Console.WriteLine("After awaiting parallelDownload <-- Everything worked OK!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            }
            finally
            {
                Console.WriteLine("In DoDownloadFileTask's finally");
                //File.Delete(outputFileName);
            }
        }
    }
}

