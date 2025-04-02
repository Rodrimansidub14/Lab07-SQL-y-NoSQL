-- Tabla para almacenar la población por país
CREATE TABLE pais_poblacion (
    id SERIAL PRIMARY KEY,
    pais VARCHAR(100) NOT NULL,
    poblacion BIGINT,
    anio INTEGER
);

-- Tabla para almacenar el índice de envejecimiento por país
CREATE TABLE pais_envejecimiento (
    id_pais INT PRIMARY KEY,
    nombre_pais VARCHAR(50) NOT NULL,
    capital VARCHAR(50),
    continente VARCHAR(50),
    region VARCHAR(50),
    poblacion REAL,
    tasa_de_envejecimiento REAL
);



SELECT DISTINCT pais 
FROM pais_poblacion
ORDER BY 1;

SELECT DISTINCT nombre_pais 
FROM pais_envejecimiento
ORDER BY 1;