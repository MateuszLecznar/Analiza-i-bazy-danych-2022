# Odpowiedzi na pytania

### Pytanie 1:
#### Ile rekordów znajduje się w tabeli nyc_streets?

``` sql
SELECT COUNT(*) FROM nyc_streets;
```

W tej tabeli znajduje się 19091 recordów.

### Pytanie 2:
#### Ile ulic w Nowym Jorku ma nazwy zaczynające się na „B”, „Q” i „M”?

``` sql
SELECT COUNT(*) FROM nyc_streets WHERE name ~ '^[B|Q|M]*';
```

W NY znajduje się 17099 ulic zaczynających się na B, Q i M.

### Pytanie 3:
#### Jaka jest populacja miasta Nowy Jork?

``` sql
SELECT SUM(popn_total) FROM nyc_census_blocks;
```

Populacja NY to 8175032.

### Pytanie 4:
#### Jaka jest populacja Bronxu, Manhattanu i Queens?

``` sql
SELECT SUM(popn_total) FROM nyc_census_blocks
WHERE boroname='The Bronx'
OR boroname='Manhattan'
OR boroname='Queens';
```

W tych trzech okręgach populacja wynosi 5201602.

### Pytanie 5:
#### Ile dzielnic ("neighborhoods") znajduje się w każdej gminie (borough)?

``` sql
SELECT 
  boroname, 
  COUNT(name) 
FROM "public"."nyc_neighborhoods"
GROUP BY boroname;
```

Liczba dzielnic:
- Manhattan: 28 <br />
- The Bronx: 24 <br />
- Staten Island: 24 <br />
- Brooklyn: 23 <br />
- Queens: 30 <br />
