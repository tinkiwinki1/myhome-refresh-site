# Myhome Tbilisi Bot

Бот отслеживает **новые объявления** на myhome.ge по квартирам в Тбилиси:
- продажа (`deal_types=1`)
- аренда (`deal_types=2`)
- тип недвижимости: квартиры (`real_estate_types=1`)

Источник данных: `https://api-statements.tnet.ge/v1/statements`.

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

Обычный режим (проверка каждые 120 секунд):

```bash
python3 myhome_tbilisi_bot.py
```

Один проход (для теста):

```bash
python3 myhome_tbilisi_bot.py --once
```

Один проход + выгрузить текущие объявления при первом запуске (без baseline):

```bash
python3 myhome_tbilisi_bot.py --once --export-initial
```

## Hosted сайт для Render

Файл `myhome_render_site.py` поднимает веб-сайт, который:
- работает как web service на Render
- даёт переключатель `Обычная (квартиры)` / `Коммерческая`
- даёт фильтры по району, area и улицам
- хранит `seen`-состояние отдельно для каждого набора фильтров
- по кнопке `Обновить объявления` показывает только новые объявления с прошлого обновления этой же локации
- после refresh сохраняет текущий срез и строит отдельную street analytics HTML со статистикой по выбранной улице/локации

Первый запуск для новой локации сохраняет текущий срез как baseline. Следующие нажатия кнопки показывают только новые объявления, которые появились после него.

Локальный запуск:

```bash
source .venv/bin/activate
python3 myhome_render_site.py
```

По умолчанию сервер слушает `0.0.0.0` и берет порт из переменной `PORT`, поэтому на Render дополнительная обвязка не нужна.

### Деплой на Render

В репозитории уже добавлен `render.yaml`:

- build command: `pip install -r requirements.txt`
- start command: `python3 myhome_render_site.py`
- health check: `/healthz`
- region: `frankfurt`
- plan: `starter`
- persistent disk: `/var/data` на `10 GB`

Важно:
- для сохранения истории и `seen`-состояния между рестартами нужен persistent disk
- в конфиге путь для данных задан через `MYHOME_DATA_DIR=/var/data`
- регион в Render потом не меняется, поэтому `frankfurt` лучше выбрать сразу
- если запускать без диска, сайт будет работать, но после рестарта потеряет базу уже просмотренных объявлений

Основные env-переменные:
- `MYHOME_PER_PAGE` — сколько объявлений брать с одной страницы API
- `MYHOME_MAX_PAGES` — сколько страниц сканировать за одно нажатие кнопки
- `MYHOME_MAX_RETURN_ITEMS` — сколько новых объявлений отдавать в UI после одной проверки

Текущий hosted-конфиг по умолчанию сканирует до `30` страниц за одно обновление.

## Что сохраняется

- SQLite база seen-ID: `state/myhome_seen.sqlite3`
- JSON файл каждого батча новых объявлений: `exports/new_listings_YYYYMMDD_HHMMSS.json`
- Общий CSV (append): `exports/new_listings_all.csv`

## Полезные параметры

- `--interval 60` — интервал опроса в секундах
- `--per-page 100` — сколько объявлений брать на страницу
- `--max-pages 30` — сколько первых страниц сканировать за цикл
- `--db-path /path/to/db.sqlite3`
- `--export-dir /path/to/exports`

## Универсальная выгрузка активных объявлений

Скрипт `export_myhome_active.py` делает разовую выгрузку **активных** объявлений по выбранному периоду и локации.

Что можно задавать:
- период через `--days` или точный диапазон `--date-from` / `--date-to`
- район через `--district-id` или по имени через `--district`
- urban/area через `--urban-id` или по имени через `--urban`
- улицу через `--street-id` или по имени через `--street`
- интерактивный сценарий через `--interactive`
- автоматическую сборку HTML после выгрузки через `--build-site`

Пример: Ваке за последние 90 дней по `urban_id=38`

```bash
source .venv/bin/activate
python3 export_myhome_active.py --urban-id 38 --days 90
```

Пример: одна улица по имени за последние 30 дней

```bash
python3 export_myhome_active.py \
  --street "Ilia Chavchavadze Avenue" \
  --days 30
```

Пример: несколько фильтров сразу

```bash
python3 export_myhome_active.py \
  --district "Vake-Saburtalo" \
  --urban "Vake" \
  --street "აბაშიძე" \
  --street "Chavchavadze" \
  --date-from 2025-12-01 \
  --date-to 2026-02-28
```

Одна команда: выбрать всё в терминале и сразу получить HTML:

```bash
python3 export_myhome_active.py --interactive --build-site
```

Одна команда без интерактива:

```bash
python3 export_myhome_active.py \
  --district "Vake-Saburtalo" \
  --urban "Vake" \
  --street "Ilia Chavchavadze Avenue" \
  --days 30 \
  --build-site
```

Что создается:
- `exports/myhome_active_<scope>_<period>_<timestamp>.csv`
- `exports/myhome_active_<scope>_<period>_<timestamp>.json`
- при `--build-site`: `exports/street_analytics_site_<scope>_<period>_<timestamp>.html`

Если запрос по `--street` неоднозначный, скрипт покажет возможные варианты с `street_id`, `urban_name` и `district_name`.
Для выбора районов и area по имени скрипт строит локальный кеш активных локаций в `state/myhome_location_catalog.json`.

## Примечание по первому запуску

По умолчанию первый запуск создает baseline (помечает уже существующие объявления как просмотренные) и **не выгружает их**.  
Чтобы выгрузить текущий срез сразу, добавьте `--export-initial`.

## Dashboard по улице Абашидзе

Автономный HTML-дешборд (открывается в браузере без Python-сервера):
- фильтры по типу сделки, дате, комнатам, площади и цене
- фильтры по `Building Status` (New/Old) и `Condition` (в т.ч. Newly Renovated)
- дедупликация похожих объявлений:
- `Relaxed` (игнорирует спальни/этаж и сглаживает мелкие расхождения) и `Strict` (учитывает этаж)
- средняя/медианная цена и цена за м2
- взвешенная цена за м2
- динамика по неделям, распределения, scatter, топ адресов
- таблица объявлений + экспорт отфильтрованного среза

Сборка:

```bash
source .venv/bin/activate
python3 build_abashidze_dashboard.py
```

После выполнения создается файл вида:

`exports/abashidze_dashboard_YYYYMMDD_HHMMSS.html`

По умолчанию скрипт берет последний CSV из `exports/` по шаблону:

`abashidze_listings_last_3_months_*.csv`

Скрипт автоматически подтягивает детали объявлений (поля `condition`, `is_old`) и кеширует их в:

`exports/abashidze_detail_cache.json`

Можно явно указать вход и выход:

```bash
python3 build_abashidze_dashboard.py \
  --input-csv exports/abashidze_listings_last_3_months_20260222_104017.csv \
  --output-html exports/abashidze_dashboard_custom.html
```

Если нужно пропустить обогащение деталями:

```bash
python3 build_abashidze_dashboard.py --skip-detail-enrichment
```

## Сайт аналитики по улицам, районам и периодам

Отдельный сайт с выбором улиц из базы:
- можно фильтровать по району (`District`)
- можно фильтровать по urban/area (`Area`)
- можно считать аналитику по одной улице
- можно выбрать сразу 2 улицы (кнопка `Top 2` или ручной выбор)
- можно считать по всей выбранной локации (чекбокс `Whole selected location`)
- можно менять период через `Date from` / `Date to`
- дедупликация похожих объявлений (`Relaxed/Strict`)

Сборка:

```bash
python3 build_street_analytics_site.py
```

Результат:

`exports/street_analytics_site_YYYYMMDD_HHMMSS.html`

По умолчанию скрипт берет последний CSV из новых универсальных экспортов `myhome_active_*.csv`.

Можно указать входной CSV и выходной HTML явно:

```bash
python3 build_street_analytics_site.py \
  --input-csv exports/myhome_active_urban_38_2025-12-01_to_2026-02-28_20260228_120000.csv \
  --output-html exports/street_analytics_site_custom.html
```

Старый рабочий функционал сохранен без изменений:
- `build_abashidze_dashboard.py`
- `exports/abashidze_dashboard_*.html`
