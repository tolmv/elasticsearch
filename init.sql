create table public.sku
(
    uuid                   uuid,
    marketplace_id         integer,
    product_id             bigint,
    title                  text,
    description            text,
    brand                  text,
    seller_id              integer,
    seller_name            text,
    first_image_url        text,
    category_id            integer,
    category_lvl_1         text,
    category_lvl_2         text,
    category_lvl_3         text,
    category_remaining     text,
    features               json,
    rating_count           integer,
    rating_value           double precision,
    price_before_discounts real,
    discount               double precision,
    price_after_discounts  real,
    bonuses                integer,
    sales                  integer,
    inserted_at            timestamp default now(),
    updated_at             timestamp default now(),
    currency               text,
    barcode                bigint,
    similar_sku            uuid[]
);

comment on column public.sku.uuid is 'id товара в нашей бд';

comment on column public.sku.marketplace_id is 'id маркетплейса';

comment on column public.sku.product_id is 'id товара в маркетплейсе';

comment on column public.sku.title is 'название товара';

comment on column public.sku.description is 'описание товара';

comment on column public.sku.category_lvl_1 is 'Первая часть категории товара. Например, для товара, находящегося по пути Детям/Электроника/Детская электроника/Игровая консоль/Игровые консоли и игры/Игровые консоли, в это поле запишется "Детям".';

comment on column public.sku.category_lvl_2 is 'Вторая часть категории товара. Например, для товара, находящегося по пути Детям/Электроника/Детская электроника/Игровая консоль/Игровые консоли и игры/Игровые консоли, в это поле запишется "Электроника".';

comment on column public.sku.category_lvl_3 is 'Третья часть категории товара. Например, для товара, находящегося по пути Детям/Электроника/Детская электроника/Игровая консоль/Игровые консоли и игры/Игровые консоли, в это поле запишется "Детская электроника".';

comment on column public.sku.category_remaining is 'Остаток категории товара. Например, для товара, находящегося по пути Детям/Электроника/Детская электроника/Игровая консоль/Игровые консоли и игры/Игровые консоли, в это поле запишется "Игровая консоль/Игровые консоли и игры/Игровые консоли".';

comment on column public.sku.features is 'Характеристики товара';

comment on column public.sku.rating_count is 'Кол-во отзывов о товаре';

comment on column public.sku.rating_value is 'Рейтинг товара (0-5)';

comment on column public.sku.barcode is 'Штрихкод';

create index sku_brand_index
    on public.sku (brand);

create unique index sku_marketplace_id_sku_id_uindex
    on public.sku (marketplace_id, product_id);

create unique index sku_uuid_uindex
    on public.sku (uuid);