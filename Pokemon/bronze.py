(spark.read
        .json('/mnt/datalake/Pokemon/pokemon')
        .createOrReplaceTempView('pokemon'))

# %%

# %sql

# # select pokemons and sort by ingestion date, then select only the last one (to avoid repeated pokemons)
# SELECT ingestion_date,
#         poke.*
# FROM pokemon
# LATERAL VIEW  explode(results) AS poke 
# QUALIFY row_number() OVER (PARTITION BY poke.name ORDER BY ingestion_date desc) = 1
# ORDER BY poke.url
# # qualify = filter for window function

# # another way to do it: (more verbose, with different queries)
# WITH all_pokes AS (
#     SELECT ingestion_date,
#             poke.*
#     FROM pokemon
#     LATERAL VIEW  explode(results) AS poke 
# )
# tb_row_number AS (
#     SELECT *,
#             row_number() OVER (PARTITION BY name ORDER BY ingestion_date DESC) AS rn_poke
#     FROM all_pokes
#     ORDER BY url, ingestion_date
# )
# SELECT * 
# FROM tb_row_number
# WHERE rn_poke = 1

query = '''
SELECT ingestion_date,
        poke.*
FROM pokemon
LATERAL VIEW  explode(results) AS poke 
QUALIFY row_number() OVER (PARTITION BY poke.name ORDER BY ingestion_date desc) = 1
ORDER BY poke.url
'''

df = spark.sql(query).coalesce(1)
df.display()

#%%

%sql

CREATE DATABASE bronze.pokemon

# %%

(df.write.format('delta')
    .mode('overwrite')
    .option('overwriteSchema', True)
    .saveAsTable('bronze.pokemon.pokemon'))


#%%

%sql

SELECT *
FROM bronze.pokemon.pokemon
