using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using InfluxMigrate.Models;
using InfluxMigrate.Filters;
using InfluxMigrate.Helpers;
using InfluxMigrate.Statics;
using InfluxData.Net;
using InfluxData.Net.Common.Enums;
using InfluxData.Net.InfluxDb;
using InfluxData.Net.InfluxDb.Infrastructure;
using InfluxData.Net.InfluxDb.Models;
using InfluxData.Net.InfluxDb.Models.Responses;

namespace InfluxMigrate.Providers
{
    public class InfluxDbProviderV9
    {
        /// <summary>
        /// Influx DB credentials / setup.
        /// </summary>
        private string _influxDbUri = ConfigurationManager.AppSettings["InfluxDb.v9.Uri"];
        private string _influxDbUserName = ConfigurationManager.AppSettings["InfluxDb.v9.UserName"];
        private string _influxDbPassword = ConfigurationManager.AppSettings["InfluxDb.v9.Password"];
        private string _influxDbName = ConfigurationManager.AppSettings["InfluxDb.v9.DbName"];

        /// <summary>
        /// Influx DB series (tables) names.
        /// </summary>
        private readonly string _influxDbReadingSeries = "reading";

        /// <summary>
        /// Default point resolution (no aggregation/groupping is done for default resolution).
        /// </summary>
        private readonly string _defaultResolution = "5s";

        /// <summary>
        /// InfluxDbClient instance.
        /// </summary>
        private InfluxDbClient _influxDbClient;

        public InfluxDbProviderV9()
        {
            _influxDbClient = new InfluxDbClient(_influxDbUri, _influxDbUserName, _influxDbPassword, InfluxDbVersion.v_0_9_6);
        }

        #region Methods

        public async Task<IInfluxDbApiResponse> SaveReading(ReadingMessageDTO dto)
        {
            var data = BuildReadingPoint(dto);
            return await WriteData(data);
        }

        public async Task<IInfluxDbApiResponse> SaveReadings(IList<ReadingMessageDTO> dtos)
        {
            IList<Point> data = new List<Point>();

            foreach (var dto in dtos)
            {
                data.Add(BuildReadingPoint(dto));
            }

            var response = await WriteData(data.ToArray());

            return response;
        }

        public Point BuildReadingPoint(ReadingMessageDTO dto)
        {
            return new Point()
            {
                Name = "reading",
                Tags = new Dictionary<string, object>()
                {
                    { InfluxSerieConstants.SensorId, dto.SensorId },
                    { InfluxSerieConstants.SerialNumber, dto.SerialNumber }
                },
                Fields = new Dictionary<string, object>()
                {
                    { InfluxSerieConstants.RawMessage, dto.RawMessage },
                    { InfluxSerieConstants.Humidity, dto.Humidity },
                    { InfluxSerieConstants.Temperature, dto.Temperature },
                    { InfluxSerieConstants.Resistance, dto.Resistance }
                },
                Timestamp = dto.DateCreated
            };
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
                var result = await QueryReading(query);
                seriesResults = result.ToList();
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
                    seriesList.Add(series.ToList());
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

        private async Task<IEnumerable<Serie>> QueryReading(string query)
        {
            return await _influxDbClient.Client.QueryAsync(_influxDbName, query);
        }

        public async Task<IInfluxDbApiResponse> WriteData(Point data)
        {
            return await _influxDbClient.Client.WriteAsync(_influxDbName, data);
        }

        public async Task<IInfluxDbApiResponse> WriteData(Point[] data)
        {
            return await _influxDbClient.Client.WriteAsync(_influxDbName, data);
        }

        public async Task<IInfluxDbApiResponse> Backfill(BackfillParams backfill)
        {
            return await _influxDbClient.ContinuousQuery.BackfillAsync(_influxDbName, backfill);
        }

        #endregion
    }
}
