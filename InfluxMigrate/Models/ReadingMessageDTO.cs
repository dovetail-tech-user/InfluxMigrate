using System;

namespace InfluxMigrate.Models
{
    public class ReadingMessageDTO
    {
        public long Timestamp { get; set; }

        public DateTime DateCreated { get; set; }

        public int SensorId { get; set; }

        public string RawMessage { get; set; }

        public string SerialNumber { get; set; }

        public int Humidity { get; set; }

        public int Temperature { get; set; }

        public int Resistance { get; set; }
    }
}
