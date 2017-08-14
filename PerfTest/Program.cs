namespace Microsoft.WindowsAzure.Storage.Blob
{
	using System; 
     	using System.IO; 
     	using System.Net; 
     	using System.Threading; 
     	using System.Diagnostics; 
     	using Microsoft.WindowsAzure.Storage; 
     	using System.Threading.Tasks;
 
    class Program
    {

        public static LargeDownloadToFile largeDownloadToFileObject;

	    public static CloudBlob GetBlob() 
        {
            string connectionString = "DefaultEndpointsProtocol=http;AccountName=accountname;AccountKey=accountkey";
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

            ServicePointManager.DefaultConnectionLimit = 100000;
            ThreadPool.SetMinThreads(Int32.MaxValue, Int32.MaxValue);
            ThreadPool.SetMaxThreads(Int32.MaxValue, Int32.MaxValue);

            //int[] parallelCounts = new int[] { 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
            //int[] rangeSizes = new int[] { 1024, 100, 16, 4 };
            //for (int i = 0; i < parallelCounts.Length; i++)
            //{
            //    for (int j = 0; j < rangeSizes.Length; j++)
            //    {
            //        if (parallelCounts[i] > 512 && rangeSizes[j] > 100)
            //        {
            //            // max I/O for 1024 is 300 so no need to test again with
            //            continue;
            //        }

            //        for (int k = 0; k < 5; k++)
            //        {
            //            Stopwatch time = Stopwatch.StartNew();
            //            DoDownloadFileTask(blob, parallelCounts[i] /*parallel IO count*/, rangeSizes[j] * 1024 * 1024 /* range size per IO */).GetAwaiter().GetResult();
            //            //Console.WriteLine("And, we're back in Main <-- YEAH !!!!!!!!!!!!!!!!!!!!");
            //            //DoParallelUploadTask().Wait();
            //            time.Stop();
            //            Console.WriteLine("Run number {0}.", k+1);
            //            Console.WriteLine("Parallel I/O Count {0}.", parallelCounts[i]);
            //            Console.WriteLine("Download size per range {0} in MB.", rangeSizes[j]);
            //            Console.WriteLine("Download has been completed in {0} seconds.", time.Elapsed.TotalSeconds.ToString());
            //        }
            //    }
            //}

            int parallelCount = 300;
            long rangeSize = 1024;
            Stopwatch time = Stopwatch.StartNew();
            DoDownloadFileTask(blob, parallelCount /*parallel IO count*/, rangeSize * 1024 * 1024 /* range size per IO */).GetAwaiter().GetResult();
            //Console.WriteLine("And, we're back in Main <-- YEAH !!!!!!!!!!!!!!!!!!!!");
            //DoParallelUploadTask().Wait();
            time.Stop();
            //Console.WriteLine("Run number {0}.", k + 1);
            Console.WriteLine("Parallel I/O Count {0}.", parallelCount);
            Console.WriteLine("Download size per range {0} in MB.", rangeSize);
            Console.WriteLine("Download has been completed in {0} seconds.", time.Elapsed.TotalSeconds.ToString());


            Console.ReadLine();
            Console.ReadLine();
        }

        //private static async Task DoParallelUploadTask()
        //{
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

            long? offset = null;
            long? length = null;
            LargeDownloadToFileSettings largeDownloadToFileSettings = new LargeDownloadToFileSettings(blob, outputFileName, FileMode.Create, offset, length, parallelCount, chunkSize);
            BlobRequestOptions options = new BlobRequestOptions();
            LargeDownloadToFile largeDownloadToFile = LargeDownloadToFile.Start(largeDownloadToFileSettings, null, null, CancellationToken.None);
            largeDownloadToFileObject = largeDownloadToFile;
            await largeDownloadToFile.Task;
        }
    }
}

