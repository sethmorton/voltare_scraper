const axios = require("axios");
const { Client } = require("pg");
const { DateTime } = require("luxon");
const dbHost = process.env.DB_HOST;
const dbName = process.env.DB_NAME;
const dbUser = process.env.DB_USER;
const dbPassword = process.env.DB_PASSWORD;
const dbPort = process.env.DB_PORT;
exports.handler = async (event, context) => {
  // URL for the actual graph - we won't use this, but it's a useful reference
  const graphUrl =
    "http://wwwmobile.caiso.com/Web.Service.Chart/pricecontourmap.html";

  // Define the URL for data scraping
  const dataUrl =
    "http://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1";

  // Define the PostgreSQL connection URI (from environment variables)

  const client = new Client({
      host: dbHost,
      port: dbPort,
      database: dbName,
      user: dbUser,
      password: dbPassword,
  })

    await client.connect();

  // Retrieve data from the URL and parse it as JSON
  const response = await axios.get(dataUrl);
  const priceMap = response.data;

  const REAL_TIME_KEY = "f";
  const rawDate = priceMap[REAL_TIME_KEY + "d"];
  const startingHour = parseInt(priceMap[REAL_TIME_KEY + "h"]) - 1;
  const endingHour = parseInt(priceMap[REAL_TIME_KEY + "h"]);
  const rawInterval = priceMap[REAL_TIME_KEY + "i"];
  const priceLayer = priceMap["l"];

  let executionArray = [];

  console.log("Raw Date:", rawDate);
  console.log("Starting Hour:", startingHour);
  console.log("Ending Hour:", endingHour);
  console.log("Raw Interval:", rawInterval);

  // Iterate through the price layer data
  for (const level of priceLayer) {
    for (const place of level["m"]) {
      const title = place["n"];
      const type = place["p"];
      const region = place["a"];
      const realTimePrice = place[REAL_TIME_KEY + "p"];
      const congestionPrice = place[REAL_TIME_KEY + "g"];
      const costPrice = place[REAL_TIME_KEY + "c"];
      const lossPrice = place[REAL_TIME_KEY + "l"];
      const iconKey = place[REAL_TIME_KEY + "k"];
      const latitude = place["c"][0];
      const longitude = place["c"][1];

      const insertQuery = `
      INSERT INTO electricity_prices (
          raw_date,
          starting_hour,
          ending_hour,
          raw_interval,
          title,
          type,
          region,
          real_time_price,
          congestion_price,
          cost_price,
          loss_price,
          icon_key,
          latitude,
          longitude,
          time_recorded
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW());
  `;

      const DATE = DateTime.fromISO(rawDate);
      const RAW_SQL_DATE = DATE.toSQLDate();
      const dataValues = [
        RAW_SQL_DATE,
        startingHour,
        endingHour,
        rawInterval,
        title,
        type,
        region,
        Number(realTimePrice),
        Number(congestionPrice),
        Number(costPrice),
        Number(lossPrice),
        iconKey,
        Number(latitude),
        Number(longitude),
      ];
      try {
          await client.query(insertQuery, dataValues);
          console.log("Inserted data for:", title);
      } catch (error) {
          console.error("Error inserting data:", error);
      }
    }
  }

    await client.end();
  return "Success!";
};

