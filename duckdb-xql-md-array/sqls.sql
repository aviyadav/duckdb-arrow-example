SELECT f.model, f.prediction_timedelta AS lead,
       SQRT(AVG(POWER(f."2m_temperature" - e."2m_temperature", 2))) AS rmse
FROM forecasts f
JOIN era5 e
  ON e.time = f.time + f.prediction_timedelta
  AND e.latitude = f.latitude
  AND e.longitude = f.longitude
GROUP BY f.model, f.prediction_timedelta
