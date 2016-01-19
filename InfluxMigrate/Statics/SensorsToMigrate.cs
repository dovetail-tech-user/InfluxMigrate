using System.Collections.Generic;

namespace InfluxMigrate.Statics
{
    public static class SensorsToMigrate
    {
        public static IDictionary<string, int> FromV8ToV9Test = new Dictionary<string, int>()
        {
            { "0000AAAA", 1 }
        };

        public static IDictionary<string, int> FromV8ToV9 = new Dictionary<string, int>()
        {
            // SAMPLE, add your identifiers here (you could also use IList instead of IDictionary
            // if your case does nto require external identifiers
            { "0000AAAA", 1 },
            { "0000BBBB", 2 },
            { "0000CCCC", 3 },
            { "0000DDDD", 4 }
        };
    }
}
