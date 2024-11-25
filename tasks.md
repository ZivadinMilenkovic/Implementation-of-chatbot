## Testiranje istih pitanja da bi se ustanovilo zasto se razlikuje broj tokena (testiranje je obavljeno kad smo imali jednu tabelu, koriscena su 2 DS3_v2 workera)
- Ukoliko se ogovor nadje isprve (koriscenjem jednog query-a) onda ce biti manja kolicina tokena

- Ukoliko agent konstantno pukusava da okene query(kako bi dobio rezultat), parsira odgovor... onda ce biti veca kolicina tokena. Ukoliko dodje od "Agent stopped due to iteration limit or time limit" ta ce se potrositi najvise tokena

- Nekad ce agent pronaci odgovor nekad ne, iako je u pitanju isto pitanje (trenutno ne znamo zasto)

- Testiranje 1: u 50% slucajeva smo dobili rezutat, u proseku 8k tokena, kada nismo dobili rez tj dobili smo "Agent stopped due to iteration limit or time limit" koristili smo ko 30k  tokena zbog kolicine iteracija

- Testiranje 2: u 70% slucajeva smo dobili rezutat, u proseku 8k tokena, kada nismo dobili rez tj dobili smo "Agent stopped due to iteration limit or time limit" koristili smo ko 30k  tokena zbog kolicine iteracija
## Da li se povecava brzina bota ako se poveca cluster



## Kako kolicina podataka utic na brzinu, cenu i preciznost

## Ubacivanje jos neke table da vidimo kako to utice na agenta

## Poboljsanje imenovanja

## Sta sve utice na brzinu dobijanja odgovora
