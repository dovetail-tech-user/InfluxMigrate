using System.Collections.Generic;

namespace InfluxMigrate.Statics
{
    public static class DownsampleIntervalConstants
    {
        public const string ThirtySeconds = "30s";
        public const string OneMinute = "1m";
        public const string FiveMinutes = "5m";
        public const string OneHour = "1h";
        public const string OneDay = "1d";
        public const string OneWeek = "1w";

        public static IList<string> AllIntervals = new List<string>()
        {
            ThirtySeconds,
            OneMinute,
            FiveMinutes,
            OneHour,
            OneDay
            //OneWeek // this one needs to be run separately because it requires a larger timeframe for better precision
        };
    }
}
