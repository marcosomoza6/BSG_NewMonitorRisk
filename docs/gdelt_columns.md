##########################################################################################################
# Archivo     : gdelt_columns.md                                                                         #
# Nombre      : Marco Somoza                                                                             #
# Descripción : Data Dictionary del archivo GDELT Events (58 columnas) + reference country_risk.         #
#                                                                                                        #
##########################################################################################################
# GDELT 2.0 Events – Data Dictionary (TSV 58 columnas)

Este proyecto usa el archivo de eventos de GDELT (events export) en formato tab-delimited.
Cada registro contiene **58 campos** (0..57). :contentReference[oaicite:6]{index=6}

## Formato del archivo (events)
- Delimitador: TAB (`\t`)
- Header: no (Spark asigna `_c0`, `_c1`, ..., `_c57`)
- Extensión: a veces viene como `.csv` pero **es TSV** (separado por TAB). :contentReference[oaicite:7]{index=7}

## Columnas (por índice 0..57)

### Identidad / tiempo
0. **GlobalEventID**: identificador único del evento (PK lógico).
1. **SQLDATE**: fecha del evento en `YYYYMMDD` (ej. 20250124).
2. **MonthYear**: `YYYYMM`.
3. **Year**: `YYYY`.
4. **FractionDate**: año con fracción (para series temporales).

### Actor1 (quién inicia la acción)
5. Actor1Code: código CAMEO del actor 1.
6. Actor1Name: nombre del actor 1 (si se reconoce).
7. Actor1CountryCode: país (actor 1) (CAMEO).
8. Actor1KnownGroupCode: grupo conocido (actor 1).
9. Actor1EthnicCode: etnicidad (actor 1).
10. Actor1Religion1Code
11. Actor1Religion2Code
12. Actor1Type1Code
13. Actor1Type2Code
14. Actor1Type3Code

### Actor2 (quién recibe)
15. Actor2Code
16. Actor2Name
17. Actor2CountryCode
18. Actor2KnownGroupCode
19. Actor2EthnicCode
20. Actor2Religion1Code
21. Actor2Religion2Code
22. Actor2Type1Code
23. Actor2Type2Code
24. Actor2Type3Code

### Evento (qué pasó)
25. **IsRootEvent**: 1 si es evento principal, 0 si es derivado.
26. **EventCode**: código CAMEO específico (3 dígitos).
27. **EventBaseCode**: base del EventCode.
28. **EventRootCode**: raíz jerárquica del evento.
29. **QuadClass**: clase 1..4 (macro-categoría). (✅ usada en Risk Weight)
30. **GoldsteinScale**: score de cooperación/conflicto (aprox -10..+10). (✅ usada como proxy de intensidad)
31. NumMentions: cuántas menciones.
32. NumSources: cuántas fuentes.
33. NumArticles: cuántos artículos.
34. AvgTone: tono promedio.

### Geo Actor1 (ubicación asociada a Actor1)
35. Actor1Geo_Type
36. Actor1Geo_Fullname
37. Actor1Geo_CountryCode
38. Actor1Geo_ADM1Code
39. Actor1Geo_ADM2Code
40. Actor1Geo_Lat
41. Actor1Geo_Long
42. Actor1Geo_FeatureID

### Geo Actor2 (ubicación asociada a Actor2)
43. Actor2Geo_Type
44. Actor2Geo_Fullname
45. Actor2Geo_CountryCode
46. Actor2Geo_ADM1Code
47. Actor2Geo_ADM2Code
48. Actor2Geo_Lat
49. Actor2Geo_Long
50. Actor2Geo_FeatureID

### ActionGeo (ubicación principal del evento)  ✅ usamos este bloque
51. **ActionGeo_Type**: tipo de geocoding (nivel).
52. **ActionGeo_Fullname**: nombre completo del lugar (ej. "Mississippi, United States"). ✅ (de aquí sacamos city)
53. **ActionGeo_CountryCode**: país (FIPS/ISO según feed). ✅ (country)
54. **ActionGeo_ADM1Code**: estado/provincia (ej. USMS). ✅ (adm1)
55. **ActionGeo_Lat**: latitud. ✅
56. **ActionGeo_Long**: longitud. ✅
57. **DateAdded / SourceURL**: en este export, las últimas posiciones representan:
    - DateAdded (YYYYMMDD)
    - SourceURL (URL del artículo)
   (En algunos codebooks aparecen como 2 columnas separadas al final; en tu archivo se observan ambas dentro del rango 56..57.)

> Nota: el codebook oficial describe el significado general de estos campos, pero existen variaciones menores por “export/feed”.
> En tu pipeline trabajamos con el layout verificado de 58 columnas (0..57). :contentReference[oaicite:8]{index=8}

## Campos que usamos en el pipeline (y por qué)
- GlobalEventID (idx 0): deduplicación (Silver).
- SQLDATE (idx 1): `event_date` (partición Silver).
- QuadClass (idx 29): scoring rápido 1..4 (risk_weight).
- GoldsteinScale (idx 30): proxy de intensidad del evento (avg_goldstein en Gold).
- ActionGeo_Fullname/Country/ADM1/Lat/Long (idx 52..56 en esta lista): localización principal (country/city + geo).

## Country Risk Reference (archivo propio del proyecto)
Archivo: gdelt_country_risk_YYYYMMDD.csv
Columnas:
- country (2 letras): llave del join (MX, US, etc.)
- baseline_risk (0..1): riesgo estructural del país (opcional en scoring futuro)
- risk_multiplier: factor multiplicador para ajustar risk_weight
- updated_at: control de versión / auditoría
