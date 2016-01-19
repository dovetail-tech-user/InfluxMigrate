using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using InfluxMigrate.Models;
using InfluxDB.Net;
using InfluxDB.Net.Models;
using InfluxMigrate.Filters;
using InfluxMigrate.Helpers;
using InfluxMigrate.Statics;

namespace InfluxMigrate.Providers
{
    public class InfluxDbProviderV8
    {
        /// <summary>
        /// Influx DB credentials / setup.
        /// </summary>
        private string _influxDbUri = ConfigurationManager.AppSettings["InfluxDb.v8.Uri"];
        private string _influxDbUserName = ConfigurationManager.AppSettings["InfluxDb.v8.UserName"];
        private string _influxDbPassword = ConfigurationManager.AppSettings["InfluxDb.v8.Password"];
        private string _influxDbName = ConfigurationManager.AppSettings["InfluxDb.v8.DbName"];

        /// <summary>
        /// Influx DB series (tables) names.
        /// </summary>
        private readonly string _influxDbReadingSeries = "reading";

        /// <summary>
        /// Default point resolution (no aggregation/groupping is done for default resolution).
        /// </summary>
        private readonly string _defaultResolution = "5s";

        /// <summary>
        /// InfluxDb instance.
        /// </summary>
        private InfluxDb _influxDbClient;

        public InfluxDbProviderV8()
        {
            _influxDbClient = new InfluxDb(_influxDbUri, _influxDbUserName, _influxDbPassword);
        }

        #region Methods

        public async Task<InfluxDbApiResponse> SaveReading(ReadingMessageDTO dto)
        {
            var data = BuildReadingSerie(dto);
            return await WriteData(TimeUnit.Milliseconds, data);
        }

        public async Task<InfluxDbApiResponse> SaveReadings(IList<ReadingMessageDTO> dtos)
        {
            IList<Serie> data = new List<Serie>();

            foreach (var dto in dtos)
            {
                data.Add(BuildReadingSerie(dto));
            }

            return await WriteData(TimeUnit.Milliseconds, data.ToArray());
        }

        public Serie BuildReadingSerie(ReadingMessageDTO dto)
        {
            return new Serie.Builder(_influxDbReadingSeries).
                Columns(
                    InfluxSerieConstants.SensorId,
                    InfluxSerieConstants.RawMessage,
                    InfluxSerieConstants.SerialNumber,
                    InfluxSerieConstants.Humidity,
                    InfluxSerieConstants.Temperature,
                    InfluxSerieConstants.Resistance)
                .Values(
                    dto.SensorId,
                    dto.RawMessage,
                    dto.SerialNumber,
                    dto.Humidity,
                    dto.Temperature,
                    dto.Resistance).
                Build();
        }

        /// <summary>
        /// Fetches readings for single or multiple sensores in a batched query. If the batched query fails,
        /// a fallback query which fetches readings sensor per sensor will get executed.
        /// </summary>
        /// <param name="filter">Fetch filter.</param>
        /// <returns></returns>
        public async Task<IList<Serie>> GetReadings(ReadingsFetchFilter filter)
        {
            IList<Serie> seriesResults = new List<Serie>();
            var errorOccured = false;

            try
            {
                var query = GetQuery(filter.SensorSerials, filter);
                seriesResults = await QueryReading(query);
            }
            catch (Exception e)
            {
                errorOccured = true;
            }

            if (errorOccured)
                seriesResults = await GetReadingsOneByOneFallback(filter);

            seriesResults.ToList().ForEach(CleanUpSensorSerial);
            return seriesResults;
        }

        /// <summary>
        /// Fetches readings sensor per sensor. If fetch is for a single sensor, it will rethrow the exception, if not,
        /// it will return, an empty Serie List. Because of this, batched queries will not "fail". They simply won't
        /// include readings for that sensor (for example on custom charts, or in AQR report). In case of a single sensor
        /// fetch - we actually want to know that the sensor is "faulty".
        /// </summary>
        /// <param name="filter">Fetch filter.</param>
        /// <returns>Async task which returns a list of seies.</returns>
        private async Task<IList<Serie>> GetReadingsOneByOneFallback(ReadingsFetchFilter filter)
        {
            IList<IList<Serie>> seriesList = new List<IList<Serie>>();
            var isSingleFetch = filter.SensorSerials.Count() == 1;

            foreach (var sensorSerial in filter.SensorSerials)
            {
                var query = GetQuery(sensorSerial, filter);
                try
                {
                    var series = await QueryReading(query);
                    seriesList.Add(series);
                }
                catch (Exception)
                {
                    if (isSingleFetch)
                    {
                        throw;
                    }
                    else
                    {
                        seriesList.Add(new List<Serie>());
                    }
                }
            }

            return FlattenSeriesResult(seriesList);
        }

        #endregion

        #region Helper Methods

        private IList<Serie> FlattenSeriesResult(IList<IList<Serie>> seriesResults)
        {
            return seriesResults.SelectMany(p => p).ToList();
        }

        private void CleanUpSensorSerial(Serie serie)
        {
            serie.Name = serie.Name.Substring(serie.Name.LastIndexOf('.') + 1);
        }

        #endregion

        #region Query builders

        private string GetQuery(string sensorSerial, ReadingsFetchFilter filter)
        {
            var seriesName = GetSeriesName(sensorSerial, filter);
            return GetBaseQuery(seriesName, filter);
        }

        private string GetQuery(IEnumerable<string> sensorSerials, ReadingsFetchFilter filter)
        {
            var seriesNames = GetSeriesNames(sensorSerials, filter);
            return GetBaseQuery(seriesNames, filter);
        }

        /// <summary>
        /// Gets base query for fetching sensors.
        /// 
        /// NOTE: SHOULD ONLY BE CALLED FROM GetQuery methods, and not any other "higher-level" methods from the chain.
        /// </summary>
        /// <param name="seriesToQuery">Series to query already prepared as a comma delimited string array.</param>
        /// <param name="filter">Readings fetch filter.</param>
        /// <returns>Base query string.</returns>
        private string GetBaseQuery(string seriesToQuery, ReadingsFetchFilter filter)
        {
            return String.Format("select {0} from {1} where time > '{2}' and time < '{3}'", filter.Metric, seriesToQuery, filter.TimeFrom, filter.TimeTo);
        }

        private string GetSeriesName(string sensorSerial, ReadingsFetchFilter filter)
        {
            var isResolutionQuery = IsResolutionQuery(filter.Resolution);
            var seriesName = String.Format("{0}.{1}", _influxDbReadingSeries, sensorSerial);

            if (isResolutionQuery)
                seriesName = String.Format("{0}.{1}", filter.Resolution, seriesName);

            return seriesName;
        }

        private string GetSeriesNames(IEnumerable<string> sensorSerials, ReadingsFetchFilter filter)
        {
            var seriesNames = sensorSerials.Select(p => GetSeriesName(p, filter)).Distinct();
            return String.Join(", ", seriesNames);
        }

        private bool IsResolutionQuery(string resolution)
        {
            return !String.IsNullOrEmpty(resolution) && resolution != _defaultResolution;
        }

        #endregion

        #region Influx communication

        private async Task<IList<Serie>> QueryReading(string query)
        {
            return await _influxDbClient.QueryAsync(_influxDbName, query, TimeUnit.Milliseconds);
        }

        public async Task<InfluxDbApiResponse> WriteData(TimeUnit timeUnit, Serie data)
        {
            return await _influxDbClient.WriteAsync(_influxDbName, timeUnit, data);
        }

        public async Task<InfluxDbApiResponse> WriteData(TimeUnit timeUnit, Serie[] data)
        {
            return await _influxDbClient.WriteAsync(_influxDbName, timeUnit, data);
        }

        #endregion
    }
}
