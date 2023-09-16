using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CTree
{
    public enum PatchLockerType
    {
        Reader,
        Writer
    }

    public class PatchLockerHandle : IDisposable
    {
        private PatchLockerType _type;
        private string _dbKey;
        public PatchLockerHandle(PatchLockerType type, string dbKey)
        {
            _type = type;
            _dbKey = dbKey;
        }

        public void Dispose()
        {
            if (_type == PatchLockerType.Reader)
                PatchLocker.ReleaseOneReader(_dbKey);
            else
                PatchLocker.ReleaseOneWriter(_dbKey);
        }
    }

    internal class PatchLockerContainer
    {
        public string _dbKey = "";
        public int _threadId = 0;

        public int _numReaders = 0;
        public int _numWriters = 0;
        public int _numWritersInQueue = 0;
    }

    public class PatchLocker
    {
        private static object _lockObjDict = new object();
        private static List<PatchLockerContainer> _containers = new List<PatchLockerContainer>();

        // NOTE!! Must be called from inside lock(_lockObjDict)
        private static PatchLockerContainer GetContainer(string dbKey)
        {
            var c = _containers.Where(p => p._dbKey == dbKey && p._threadId == Thread.CurrentThread.ManagedThreadId).SingleOrDefault();
            if (c == null)
            {
                c = new PatchLockerContainer
                {
                    _dbKey = dbKey,
                    _threadId = Thread.CurrentThread.ManagedThreadId
                };
                _containers.Add(c);
            }
            return c;
        }

        // NOTE!! Must be called from inside lock(_lockObjDict)
        private static bool IsAnyoneElseWriting(string dbKey)
        {
            foreach (var c in _containers)
            {
                // Threads working with other data sources are not affecting us...
                if (c._dbKey != dbKey)
                    continue;

                // Stuff going on in our own thread is not affecting us either.
                if (c._threadId == Thread.CurrentThread.ManagedThreadId)
                    continue;

                // If there are active writers - return true.
                if (c._numWriters > 0)
                    return true;
            }

            // Nothing is going on with writing, safe to continue!
            return false;
        }


        // NOTE!! Must be called from inside lock(_lockObjDict)
        private static bool IsAnyoneElseWritingOrStandingInQueToWrite(string dbKey)
        {
            foreach (var c in _containers)
            {
                // Threads working with other data sources are not affecting us...
                if (c._dbKey != dbKey)
                    continue;

                // Stuff going on in our own thread is not affecting us either.
                if (c._threadId == Thread.CurrentThread.ManagedThreadId)
                    continue;

                // If there are active writers or writers in queue - return true.
                if (c._numWriters > 0 || c._numWritersInQueue > 0)
                    return true;
            }

            // Nothing is going on with writing, safe to continue!
            return false;
        }

        public static PatchLockerHandle WaitUntilOkayToRead(string dbKey)
        {
            lock (_lockObjDict)
            {
                var c = GetContainer(dbKey);
                if (c._numReaders > 0 || c._numWriters > 0)
                {
                    // We already have an active reader... add ourselves as reader and simply return.
                    c._numReaders++;
                    var obj = new PatchLockerHandle(PatchLockerType.Reader, dbKey);
                    return obj;
                }
            }

            while (true)
            {
                lock (_lockObjDict)
                {
                    // If there are active writers or writers standing in queue - wait 100 ms and try again...
                    if (IsAnyoneElseWritingOrStandingInQueToWrite(dbKey))
                    {
                        goto cntnue;
                    }

                    // No one else is writing - continue to add a reader to my container.
                    var c = GetContainer(dbKey);

                    c._numReaders++;
                    var obj = new PatchLockerHandle(PatchLockerType.Reader, dbKey);
                    return obj;
                }
            cntnue:
                Thread.Sleep(100);
            }
        }

        public static void ReleaseOneReader(string dbKey)
        {
            lock (_lockObjDict)
            {
                var c = GetContainer(dbKey);
                c._numReaders--;
                if (c._numReaders == 0 && c._numWriters == 0 && c._numWritersInQueue == 0)
                {
                    _containers.Remove(c);
                }
            }
        }

        public static void ReleaseOneWriter(string dbKey)
        {
            lock (_lockObjDict)
            {
                var c = GetContainer(dbKey);
                c._numWriters--;
                if (c._numReaders == 0 && c._numWriters == 0 && c._numWritersInQueue == 0)
                {
                    _containers.Remove(c);
                }
            }
        }

        // NOTE!! Must be called from inside lock(_lockObjDict)
        private static bool IsAnyoneElseReading(string dbKey)
        {
            foreach (var c in _containers)
            {
                // Threads working with other data sources are not affecting us...
                if (c._dbKey != dbKey)
                    continue;

                // Stuff going on in our own thread is not affecting us either.
                if (c._threadId == Thread.CurrentThread.ManagedThreadId)
                    continue;

                // If there are active readers - return true.
                if (c._numReaders > 0)
                    return true;
            }

            // Nothing is going on with writing, safe to continue!
            return false;
        }

        public static PatchLockerHandle WaitUntilOkayToWrite(string dbKey)
        {
            lock (_lockObjDict)
            {
                var c = GetContainer(dbKey);
                if (c._numWriters > 0)
                {
                    // We already have an active writer... add ourselves as writer and simply return.
                    c._numWriters++;
                    var obj = new PatchLockerHandle(PatchLockerType.Writer, dbKey);
                    return obj;
                }

                // Otherwise, stand in queue and wait for a free slot
                c._numWritersInQueue++;
            }

            var startTimeInQueue = DateTime.Now;

            while (true)
            {
                lock (_lockObjDict)
                {
                    if ((DateTime.Now - startTimeInQueue).TotalSeconds < 10)
                    {
                        // If there are active readers - wait 100 ms and try again... let them finish first. No new readers will
                        // be added because we are already standing in queue.
                        // NOTE!! If anyone else has been reading for more than 10 seconds... something has gone wrong!
                        if (IsAnyoneElseReading(dbKey))
                        {
                            //Thread.Sleep(100);
                            //continue;
                            goto cntnue;
                        }
                    }

                    // Same thing if someone else is writing....
                    if (IsAnyoneElseWriting(dbKey))
                    {
                        goto cntnue;
                    }

                    // No one else is reading - continue to add a writer to my container.
                    var c = GetContainer(dbKey);

                    c._numWritersInQueue--; // We're no longer waiting in queue
                    c._numWriters++;
                    var obj = new PatchLockerHandle(PatchLockerType.Writer, dbKey);
                    return obj;
                }
            cntnue:
                Thread.Sleep(100);
            }
        }
    }
}