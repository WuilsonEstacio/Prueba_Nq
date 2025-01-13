# Detección de Fraccionamiento Transaccional

Este documento describe el proceso seguido para **explorar** los datos, **identificar** variables relevantes y **definir** un modelo heurístico para detectar la práctica del **Fraccionamiento Transaccional**, entendida como la división de una transacción grande en varias más pequeñas dentro de una misma ventana de 24 horas.

---

## Tabla de Contenido
1. [Introducción](#introducción)  
2. [Objetivo](#objetivo)  
2. [Metodo](#Metodo) 
3. [Diccionario de Variables](#Diccionario-de-Variables)
4. [EDA](#EDA)  
   - [Data Quality](#Data-Quality)  
   - [Estadisticas](#Estadisticas)  
   - [Hipótesis preliminares](#hipótesis-preliminares)  
5. [Flujo Modelo Analítico](#Flujo-Modelo-Analítico)  
   - [Data flow](#fData_flow)
   - [Criterio de Selección del Modelo Analítico](#criterio-de-selección-del-modelo-analítico)
   - [Lógica de Fraccionamiento](#lógica-de-fraccionamiento) -
   - [Frecuencia de Actualización](#frecuencia-de-actualización)
6. [Resultados](#resultados)
   - [Distribución global de transacciones](#1-distribución-global-de-transacciones)
   - [Densidad de montos promedios](#2-densidad-de-montos-promedios)
   - [Transacciones fraccionadas por dia de la semana](#3-transacciones-fraccionadas-por-día-de-la-semana)
   - [Heatmap dia vs hora transacciones fraccionadas](#4-heatmap-día-vs-hora-transacciones-fraccionadas)
   - [Porcentaje de transacciones fraccionadas por tipo](#5-porcentaje-de-transacciones-fraccionadas-por-tipo)
   - [Top 10 usuarios con mas transacciones fraccionadas](#6-top-10-usuarios-con-más-transacciones-fraccionadas)
7. [Conclusiones](#conclusiones)
   - [Implementación](#1-implementación)
   - [Trazabilidad](#2-trazabilidad)
   - [Monitoreo](#3-monitoreo)
   - [Próximos pasos](#4-próximos-pasos)

---

## Introducción
Se desea dar solucion a la prueba tecnica, y para ello se genera un enlace de donde se encuentran los datos, que contienen informacion de las transaciones, donde la pruba consiste en **Encontrar y detectar patrones** de mala practica transacional en este caso **Fracionamiento transaccional**  donde se entiende en este caso que es una practica que consiste en fraccional o dividir  una transaccion grande en multiples pequeñas, estas se caracterizan por estar en una ventana que suele ser de 24 horas y que tienen como origen o destino la misma cuenta.

---

## Objetivo
El objetivo de la prueba es idear una solución para identificar transacciones que evidencian un 
comportamiento de Mala Práctica Transaccional, empleando un producto de datos. Adicional, 
describir la solución y detallar cómo incorporar el producto de datos en un marco operativo.

## Metodo
Mediante el EDA que es la exploracion de los datos y analisis. se espera que se saque la propuesta para el modelo y sus reglas, ademas de que se diseñe la arquitectura para despegar la solucion.

---

### Diccionario de Variables

- **`_id`**: Identificador único del registro.
- **`merchant_id`**: Código único del comercio o aliado.
- **`subsidiary`**: Código único de la sede o sucursal.
- **`transaction_date`**: Fecha de transacción en el core financiero.
- **`account_number`**: Número único de cuenta.  
- **`user_id`**: Código único del usuario dueño de la cuenta desde donde se registran las transacciones.
- **`transaction_amount`**: Monto de la transacción (en moneda ficticia).
- **`transaction_type`**: Naturaleza de la transacción (crédito o débito).

---

## EDA
En esta parte se trabajo con el 60% de una de las bases, que reprecentan un **6.456.562 de filas**  y  **1.570.006 de clientes unicos** por capacidad de procesamieto, no se decidio unir los dos datasets.

### Data Quality
1. No se identificaron nombres de columnas en mal estado pero mas sin embargo por si en proximas oportunidades viene con caracteres especiales o cosas asi se creo codigo que las mejore.
2. No se encontraron valores nulos.
3. se observa en algunos casos que algunos usuarios tiene el mismo numero de cuenta.
4. No se observa duplicidad en el _id que debe ser unico.
5. Se observa duplicidad entre el numero de usuario user_id, y la misma fecha hora de transaccion transaction_date, los cuales fueron eliminados


### Estadisticas
- **Montos (`transaction_amount`)**  
  - Máximo: 3210.00
  - Mínimo: 5.94
  - Promedio:  178.46 
  - Desviación Estándar: ~263.35

- **Distribución por tipo (`transaction_type`)**  
  - Débito  x= 1301261, representa el 0.79 del total de la data
  - Crédito x= 5155223, representa el 0.21 del total de la data

- **Diferencia proedio en horas por transaccion (`avg_diff_hours`)**
  - Máximo: 23.98
  - Mínimo: 0.0002
  - Promedio:  1.41 
- **Diferencia promedio en minutos por transaccion (`avg_diff_minutes`)**
  - Máximo:   1438.75
  - Mínimo:   0.02
  - Promedio: 85.086
- **Usuarios con mas de 3 transaciones en un dia o ventada de 24 horas**
   -  se encuentran `43.571` usuarios unicos que posiblemente estan realizando Fracionamiento transaccional
      estos se pueden separar en dos grupos los que acumulan un monto total menor de 3000 dolares o mayor e igual a 3000 dolares en sus transaciones
   - De los datos podemos ver que para las transacciones totales menores a 3000 dólares, el tiempo promedio entre transacciones es de 44.32 minutos. y que el 50% de estas transacciones se realizaron con una diferencia de tiempo entre ellas menor a 5.79 minutos.
   - De los datos podemos ver que para las transacciones totales mayores e iguales a 3000 dólares, el tiempo promedio entre transacciones es de 13.67 minutos. y que el 50% de estas transacciones se realizaron con una diferencia de tiempo entre ellas menor a 1.47 minutos

### Hipótesis preliminares
1. **Fraccionamiento por conteo**: Si un usuario hace más de 2 transacciones en 24h, podría ser considerado como  que esta realizando fraccionamiento transaccional, se toma mas de 2 porque la segunda pudo haber sido un error en el pago o le falto pagar la diferencia o n casos asi.
---

## Flujo Modelo Analítico

### Data flow
```plaintext
┌─────────────────────────────────────┐
│  Pasos para un desarrollo efectivo  │    
└─────────────────────────────────────┘
          │
          ▼
┌──────────────────────┐
│  Entendimiento       │ 
│   del problema       │
└──────────────────────┘
          │
          ▼
┌──────────────────────┐
│  DESCARGA DE DATOS   │ 
│  (Ingesta de Datos)  │
└──────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────┐
│1. RECEPCIÓN DE DATOS (ARCHIVOS CSV, PARQUET, ETC.) │
│   - Lectura de los ficheros                        │
│   - Otras fuentes de datos                         │
└────────────────────────────────────────────────────┘
          │
          ▼
┌───────────────────────────────────────────────────────┐
│2. PREPROCESAMIENTO Y VALIDACIÓN                       │
│   - Limpieza de registros (valores nulos, duplicados) │
│   - Formateo de fechas (datetime)                     │
│   - Conversión de tipos                               │
│   - Garantizar la integridad de los datos             │
└───────────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────────────────────────────────────┐
│3. APLICACIÓN DE LA LÓGICA (REGLA DE NEGOCIOS)    │
│   - Para cada transacción, verificar si cumple   │ 
│     el criterio de fraccionamiento en 24h        │
│   - Crear la columna “windows_time”  donde si    |
|     aparece almenos dos veces el mismo inicados  |
|     es que fue fracionada, osea si el indicados  |
|    1 aparece tres veces quiere decir que esa     |
|     cuenta fue fracionada 3 veces en mesnoo      |
|     de 24 horas                                  │
└──────────────────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────────────────┐
│4. GENERACIÓN DE ATRIBUTOS (FEATURES)                           │
│   - Calculo de diferencia en horas y minutos por transaccion   │
│   - Cálculo de montos promedios                                │
│   - identificacion de usuarios con mas de 2 transacciones      |
|                     en menos de 24 horas                       |
|   - Comportamiento de variables para esos clientes              │
└────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│5. SALIDA                                                    │
│   - Almacenamiento de resultados (CSV, Base de datos, etc.) │
│   - Consumir los resultados en dashboards                   │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌───────────────────┐
│  FIN DEL PROCESO  │
└───────────────────┘
```

![Data flow](./imagenes/flujo.png "Flujo Proceso")
