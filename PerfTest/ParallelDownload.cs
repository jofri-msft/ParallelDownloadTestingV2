
//using Microsoft.WindowsAzure.Storage.Core.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
namespace Microsoft.WindowsAzure.Storage.Blob
{
    public class ParallelDownload
    {
        public readonly ParallelDownloadSettings ParallelDownloadSettings;

        public Task Task { get; private set; }

        //private int parallelIOCount;

        //private long chunkSize;

        private long? offset;

        private long? length;

        //private string path;

        //private FileMode fileMode;

        List<Stream> viewStreams;
        List<Tuple<Task, Stream>> viewStreamTasks;

        List<Task> downloadTaskList;
        enum State { Running, Paused }
        private State state = State.Running;

        private List<Task> continuationTasks = new List<Task>();

        private ParallelDownload(ParallelDownloadSettings parallelDownloadSettings)
        {
            this.ParallelDownloadSettings = parallelDownloadSettings;
            this.viewStreams = new List<Stream>();
            this.viewStreamTasks = new List<Tuple<Task, Stream>>();
            this.offset = parallelDownloadSettings.Offset;
            this.length = parallelDownloadSettings.Length;
        }

        public static ParallelDownload Start(ParallelDownloadSettings parallelDownloadSettings)
        {
            return Start(parallelDownloadSettings, null, null, null, CancellationToken.None);
        }

        public static ParallelDownload Start(ParallelDownloadSettings parallelDownloadSettings, AccessCondition accessConditions, OperationContext operationContext, BlobRequestOptions options, CancellationToken cancellationToken)
        {
            ParallelDownload parallelDownload = new ParallelDownload(parallelDownloadSettings);
            parallelDownload.Task = StartAsync(parallelDownload, accessConditions, operationContext, options, cancellationToken);

            return parallelDownload;
        }

        private static async Task StartAsync(ParallelDownload parallelDownload, AccessCondition accessConditions, OperationContext operationContext, BlobRequestOptions options, CancellationToken cancellationToken)
        {
            ParallelDownloadSettings downloadSettings = parallelDownload.ParallelDownloadSettings;
            long timeStamp = Stopwatch.GetTimestamp();
            CloudBlob blob = downloadSettings.Blob;
            if (!parallelDownload.length.HasValue)
            {
                if (blob.Properties.Length == -1)
                {
                    await blob.FetchAttributesAsync(accessConditions, options, operationContext);
                }

                parallelDownload.length = blob.Properties.Length;
            }

            if (!downloadSettings.Offset.HasValue)
            {
                parallelDownload.offset = 0;
            }

            parallelDownload.downloadTaskList = new List<Task>();
            int totalIOReadCalls = (int)Math.Ceiling((double)parallelDownload.length.Value / (double)downloadSettings.MaxRangeSize);
            Console.WriteLine($"TotalIOreadCalls={totalIOReadCalls}");
            try
            {
                //using (var fs = new FileStream(downloadSettings.FilePath, downloadSettings.FileMode, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan))
                using (MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(downloadSettings.FilePath, downloadSettings.FileMode, null, parallelDownload.length.Value/*, MemoryMappedFileAccess.ReadWrite, null, HandleInheritability.None, false*/))
                {
                    for (int i = 0; i < totalIOReadCalls; i++)
                    {
                        Console.WriteLine($"About to get block={i}");
                        if (parallelDownload.downloadTaskList.Count >= downloadSettings.ParallelIOCount)
                        {
                            try
                            {

                                // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                                Console.WriteLine("Hit max I/Os, waiting for 1 to complete");
                                Task downloadChunk = await Task.WhenAny(parallelDownload.downloadTaskList).ConfigureAwait(false);
                                await downloadChunk;
                                Console.WriteLine("An I/O completed");
                                //downloadChunk
                                parallelDownload.downloadTaskList.Remove(downloadChunk);
                                Console.WriteLine("Removed pending I/O");
                                //await parallelDownload.viewStreamTasks.Find(t => t.Item1.Id == downloadChunk.Id).Item2.FlushAsync().ConfigureAwait(false);
                                //parallelDownload.viewStreamTasks.Find(t => t.Item1.Id == downloadChunk.Id).Item2.Dispose();
                                var streamTask = parallelDownload.viewStreamTasks.Find(t => t.Item1.Id == downloadChunk.Id);
                                parallelDownload.viewStreamTasks.Remove(streamTask);
                                streamTask.Item2.Dispose();
                            }
                            catch (StorageException)
                            {
                                throw;
                            }
                        }

                        long streamBeginIndex = parallelDownload.offset.Value + (i * downloadSettings.MaxRangeSize);
                        long streamReadSize = downloadSettings.MaxRangeSize;
                        // last range may be smaller than the chunk size
                        if (i == totalIOReadCalls - 1)
                        {
                            streamReadSize = parallelDownload.length.Value - (parallelDownload.offset.Value + (i * streamReadSize));
                            Console.WriteLine("About to request the last block");
                        }

                        Console.WriteLine($"Creating MMF stream for block{i}, offset={streamBeginIndex}, size={streamReadSize}");
                        MemoryMappedViewStream viewStream = mmf.CreateViewStream(streamBeginIndex, streamReadSize);
                        //var viewStream = System.IO.Stream.Null;//..MemoryStream(new Byte[streamReadSize]);//.CreateViewStream(streamBeginIndex, streamReadSize);

                        parallelDownload.viewStreams.Add(viewStream);
                        Console.WriteLine($"Initiating download of block={i}");
                        Task downloadTask = downloadSettings.Blob.DownloadRangeToStreamAsync(viewStream, streamBeginIndex, streamReadSize, accessConditions, options, operationContext, cancellationToken);
                        parallelDownload.downloadTaskList.Add(downloadTask);
                        Console.WriteLine($"Add block={i} task to collection, outstanding I/O={parallelDownload.downloadTaskList.Count}");
                        parallelDownload.viewStreamTasks.Add(Tuple.Create(downloadTask, (Stream)viewStream));
                    }

                    Console.WriteLine($"for loop done; about to wait for all remaining I/Os={parallelDownload.downloadTaskList.Count}");
                    while (parallelDownload.downloadTaskList.Count > 0)
                    {
                        try
                        {

                            // The await on WhenAny does not await on the download task itself, hence exceptions must be repropagated.
                            Console.WriteLine($"Outside for loop waiting for remaining downloadTask count={parallelDownload.downloadTaskList.Count}");
                            Task downloadChunk = await Task.WhenAny(parallelDownload.downloadTaskList).ConfigureAwait(false);
                            await downloadChunk;
                            Console.WriteLine("An I/O completed");
                            //downloadChunk
                            parallelDownload.downloadTaskList.Remove(downloadChunk);
                            Console.WriteLine("Removed pending I/O");
                            //await parallelDownload.viewStreamTasks.Find(t => t.Item1.Id == downloadChunk.Id).Item2.FlushAsync().ConfigureAwait(false);
                            var streamTask = parallelDownload.viewStreamTasks.Find(t => t.Item1.Id == downloadChunk.Id);
                            parallelDownload.viewStreamTasks.Remove(streamTask);
                            streamTask.Item2.Dispose();
                        }
                        catch (StorageException)
                        {
                            throw;
                        }
                    }

                    //Console.WriteLine($"for loop done; about to wait for all remaining I/Os={parallelDownload.downloadTaskList.Count}");
                    //await Task.WhenAll(parallelDownload.downloadTaskList).ConfigureAwait(false);
                    Console.WriteLine("All I/Os complete");
                }
            }
            finally
            {
                Console.WriteLine($"In finally, disposing {parallelDownload.viewStreams.Count} streams");
                foreach (var viewStream in parallelDownload.viewStreams)
                {
                    viewStream.Dispose();
                }
                Console.WriteLine("leaving finally");
            }
        }
    }
}
