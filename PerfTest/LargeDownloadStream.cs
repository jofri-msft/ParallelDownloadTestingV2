using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Blob
{
    internal sealed class LargeDownloadStream : Stream
    {
        private UnmanagedMemoryStream downloadStream;
        private Timer timer;
        /// <summary>
        /// The maximum time between write calls to the stream.
        /// </summary>
        private int MAX_IDLE_TIME_MS = 120000; //30000;
        public readonly long startingRangeOffset;

        private Stopwatch stopwatch;

        public CancellationToken CancellationToken {
            get {
                return cts.Token; 
            }
        }

        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private bool disposed = false;

        public LargeDownloadStream(UnmanagedMemoryStream downloadStream, long startingRangeOffset)
        {

            this.startingRangeOffset = startingRangeOffset;
            this.downloadStream = downloadStream;
            this.timer = new Timer(
                _ => { cts.Cancel(); this.disposed = true; },
                null,
                MAX_IDLE_TIME_MS,
                Timeout.Infinite);

            //stopwatch = Stopwatch.StartNew();
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {

            this.timer.Change(MAX_IDLE_TIME_MS, Timeout.Infinite);
            //stopwatch.Stop();
            //Console.WriteLine("offset:{0} time from prev write call in {1} seconds.", startingRangeOffset, stopwatch.Elapsed.TotalSeconds.ToString());
            //stopwatch = Stopwatch.StartNew();
            return this.downloadStream.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {

            this.timer.Change(MAX_IDLE_TIME_MS, Timeout.Infinite);
            //stopwatch.Stop();
            //Console.WriteLine("offset:{0} time from prev write call in {1} seconds.", startingRangeOffset, stopwatch.Elapsed.TotalSeconds.ToString());
            //stopwatch = Stopwatch.StartNew();
            this.downloadStream.Write(buffer, offset, count);
        }

        public override void Flush()
        {
            this.downloadStream.Flush();
        }

        public override void Close()
        {
            this.timer.Dispose();
            this.cts.Dispose();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            //if (this.disposed)
            //{
            //    throw new Exception();
            //}

            return this.downloadStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            this.downloadStream.SetLength(value);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return this.downloadStream.Read(buffer, offset, count);
        }

        public override bool CanRead
        {
            get
            {
                return this.downloadStream.CanRead;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return this.downloadStream.CanSeek;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return this.downloadStream.CanWrite;
            }
        }

        public override long Length
        {
            get
            {
                return this.downloadStream.Length;
            }
        }

        public override long Position
        {
            get
            {
                return this.downloadStream.Position;
            }
            set
            {
                this.downloadStream.Position = value;
            }
        }
    }
}
