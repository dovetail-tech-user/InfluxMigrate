# InfluxMigrate
A simple .net console app for migrating data from InfluxDb v0.8 to InfluxDb v0.9.

This tool was developed to help us migrate from InfluxDb v0.8.7 to v0.9.6 because the official migration paths didn't work for us (DB upgrades would either lose data or not finish at all). The app reads series by series, day by day of data from one DB and then stores it into the second one. You could modify this behaviour to better suit your needs of course.

The app also supports running multiple backfills (if you were using them). That means that the app will go back through time and fill the rollups that you tell it to create.

You will have to change the model object (`ReadingMessageDTO`) that we worked with to suit your data schema but it trivially easy to do that. The rest of the codebase will provide you with the infrastructure to execute the migration without much work.

Make sure to enter your InfluxDb credentials into `App.config`.
