drop table if exists arg_adrese;
drop table if exists arg_adrese_arh;
drop table if exists art_vieta;
drop table if exists art_nlieta;
drop table if exists art_eka_geo;
drop table if exists art_dziv;

create table arg_adrese (
  adr_cd        bigint primary key,
  tips_cd       bigint,
  statuss       text,
  apstipr       text,
  apst_pak      int,
  std           text,
  vkur_cd       bigint,
  t1            text,
  t2            text,
  t3            text,
  t4            text,
  t5            text,
  t6            text,

  dat_sak       timestamp with time zone,
  dat_beig      timestamp with time zone,

  dat_mod       timestamp with time zone,

  sync_synced   timestamp with time zone
);

create table arg_adrese_arh (
  id            bigint primary key,
  adr_cd        bigint,
  tips_cd       bigint,
  statuss       text,
  apstipr       text,
  apst_pak      int,
  std           text,
  vkur_cd       bigint,
  t1            text,
  t2            text,
  t3            text,
  t4            text,
  t5            text,
  t6            text,

  dat_sak       timestamp with time zone,
  dat_beig      timestamp with time zone,

  dat_mod       timestamp with time zone,
  sync_synced   timestamp with time zone
);

create table art_vieta (
  kods          bigint primary key,
  tips_cd       bigint,
  apstipr       text,
  apst_pak      int,
  statuss       text,
  vkur_cd       bigint,
  vkur_tips     int,
  nosaukums     text,
  sort_nos      text,
  atrib         text,

  dat_sak       timestamp with time zone,
  dat_beig      timestamp with time zone,

  dat_mod       timestamp with time zone,
  sync_synced   timestamp with time zone

);

create table art_nlieta (
  kods          bigint primary key,
  tips_cd       bigint,
  statuss       text,
  apstipr       text,
  apst_pak      int,
  vkur_cd       bigint,
  vkur_tips     int,
  nosaukums     text,
  sort_nos      text,
  atrib         text,
  pnod_cd       bigint,

  for_build     text,

  dat_sak       timestamp with time zone,
  dat_beig      timestamp with time zone,

  dat_mod       timestamp with time zone,
  sync_synced   timestamp with time zone

);

create table art_eka_geo (
  mslink        bigint primary key,
  vieta_cd      bigint,
  koord_x       decimal(9, 3),
  koord_y       decimal(9, 3),
  sync_synced   timestamp with time zone

);

create table art_dziv (
  kods bigint primary key,
  tips_cd bigint,
  statuss text,
  apstipr text,
  apst_pak int,
  vkur_cd bigint,
  vkur_tips int,
  nosaukums text,
  sort_nos text,
  atrib text,

  dat_sak timestamp with time zone,
  dat_beig timestamp with time zone,

  dat_mod timestamp with time zone
);
