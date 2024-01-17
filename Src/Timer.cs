using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventConvergencPOCTest.Src
{
    internal class Timer
    {
        List<double> Timers;
        int nTimers;

        // private DateTime _StartTime;
        Stopwatch stopWatch = new Stopwatch();

        public Timer(int nTimers)
        {
            Timers = new List<double>(nTimers);
            this.nTimers = nTimers;
            for (int i = 0; i < nTimers; i++)
            {
                Timers.Add(0);
            }
        }

        public void Reset()
        {
            for (int i = 0; i < nTimers; i++)
            {
                Timers[i] = 0;
            }
        }

        public void StartTimer()
        {
            stopWatch.Reset();
            stopWatch.Start();
            // _StartTime = DateTime.UtcNow;
        }

        public void EndTimer(int index)
        {
            stopWatch.Stop();
            Timers[index] = stopWatch.ElapsedMilliseconds; // (DateTime.UtcNow - _StartTime).TotalSeconds;
        }

        public double GetTimer(int index)
        {
            return Timers[index];
        }

        public void SetTimer(int index, double val)
        {
            Timers[index] = val;
        }

        public void UpdateTimer(int index, double val)
        {
            Timers[index] += val;
        }
    }
}
