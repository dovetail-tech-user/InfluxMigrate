using System.Collections.Generic;

namespace InfluxMigrate.Filters
{
    public class ReadingsFetchFilter
    {
        /// <summary>
        /// A collection of sensor serial codes.
        /// </summary>
        public IEnumerable<string> SensorSerials { get; set; }

        /// <summary>
        /// Time from filter.
        /// Influx syntax expects the following format: "yyyy-MM-dd HH:mm:ss".
        /// </summary>
        public string TimeFrom { get; set; }

        /// <summary>
        /// Time to filter.
        /// Influx syntax expects the following format: "yyyy-MM-dd HH:mm:ss".
        /// </summary>
        public string TimeTo { get; set; }

        /// <summary>
        /// Density of series points. 
        /// Can be something like: "1s", "5s", "1m", "15m", "1h"...
        /// </summary>
        public string Resolution { get; set; }

        /// <summary>
        /// Metric to fetch (Temperature, Humidity...)
        /// </summary>
        public string Metric { get; set; }
    }
}