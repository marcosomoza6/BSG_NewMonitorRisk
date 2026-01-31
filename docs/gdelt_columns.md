# FLOWER BOX
# GDELT 2.0 Events – Data Dictionary (TSV 58 columnas)

Este proyecto usa la tabla de eventos de GDELT 2.0. Cada registro contiene 58 campos.  
Fuente: documentación y codebook oficial.  

## Formato del archivo
- Delimitador: TAB (`\t`)
- Header: no (se accede por posiciones `_c0`, `_c1`, …)
- Ejemplo de mapping en Spark: `_c0` = GlobalEventID, `_c1` = SQLDATE, `_c29` = QuadClass, `_c30` = GoldsteinScale, etc.

## Columnas (por índice)
- Nota: los nombres corresponden al estándar del codebook GDELT 2.0.

### Identidad y fecha
- 0 GlobalEventID: identificador único del evento
- 1 SQLDATE: fecha del evento (YYYYMMDD)
- 2 MonthYear: YYYYMM
- 3 Year: YYYY
- 4 FractionDate: año con fracción (series temporales)

### Actor1 (quién inicia)
- 5 Actor1Code
- 6 Actor1Name
- 7 Actor1CountryCode
- 8 Actor1KnownGroupCode
- 9 Actor1EthnicCode
- 10 Actor1Religion1Code
- 11 Actor1Religion2Code
- 12 Actor1Type1Code
- 13 Actor1Type2Code
- 14 Actor1Type3Code

### Actor2 (quién recibe)
- 15 Actor2Code
- 16 Actor2Name
- 17 Actor2CountryCode
- 18 Actor2KnownGroupCode
- 19 Actor2EthnicCode
- 20 Actor2Religion1Code
- 21 Actor2Religion2Code
- 22 Actor2Type1Code
- 23 Actor2Type2Code
- 24 Actor2Type3Code

### Evento (qué pasó)
- 25 IsRootEvent
- 26 EventCode (CAMEO específico)
- 27 EventBaseCode
- 28 EventRootCode
- 29 QuadClass (1..4)  ✅ (usado en Risk Weight)
- 30 GoldsteinScale (-10..+10 aprox) ✅ (proxy de impacto)
- 31 NumMentions
- 32 NumSources
- 33 NumArticles
- 34 AvgTone

### Geo Actor1
- 35 Actor1Geo_Type
- 36 Actor1Geo_Fullname
- 37 Actor1Geo_CountryCode
- 38 Actor1Geo_ADM1Code
- 39 Actor1Geo_ADM2Code
- 40 Actor1Geo_Lat
- 41 Actor1Geo_Long
- 42 Actor1Geo_FeatureID

### Geo Actor2
- 43 Actor2Geo_Type
- 44 Actor2Geo_Fullname
- 45 Actor2Geo_CountryCode
- 46 Actor2Geo_ADM1Code
- 47 Actor2Geo_ADM2Code
- 48 Actor2Geo_Lat
- 49 Actor2Geo_Long
- 50 Actor2Geo_FeatureID

### ActionGeo (geo principal del evento)
- 51 ActionGeo_Type
- 52 ActionGeo_Fullname ✅ (usado para extraer city)
- 53 ActionGeo_CountryCode ✅ (usado como country)
- 54 ActionGeo_ADM1Code ✅ (estado/provincia)
- 55 ActionGeo_ADM2Code
- 56 ActionGeo_Lat ✅
- 57 ActionGeo_Long ✅
- 58 ActionGeo_FeatureID
- 59 DateAdded
- 60 SourceURL

## Campos que usamos en el pipeline (y por qué)
- GlobalEventID: PK para deduplicar (Silver)
- SQLDATE: partición por event_date (Silver)
- QuadClass: scoring rápido 1..4 (riesgo “macro”)
- GoldsteinScale: proxy de intensidad/impacto del tipo de evento
- ActionGeo_*: ubicación principal del evento (país/estado/lat/long + nombre para ciudad)

## Country Risk Reference (archivo propio del proyecto)
Archivo: gdelt_country_risk_YYYYMMDD.csv  
Columnas:
- country (2 letras): llave del join (MX, US, etc.)
- baseline_risk (0..1): riesgo estructural del país
- risk_multiplier: factor multiplicador para ajustar risk_weight
- updated_at: control de versión / auditoría
