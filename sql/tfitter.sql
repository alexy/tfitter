create table trange (
  uid integer not null primary key, 
  first bigint not null, 
  last bigint not null, 
  total integer not null,
  declared integer not null,
  flags integer not null);

create table username (
  uid integer not null primary key,
  name varchar(32) -- NB check real maximum
  )
  
create table tweet (
  tid bigint not null primary key,
  uid integer not null,
  reply_tid bigint,
  reply_uid integer,
  created_at date not null,
    
-- Tweet
-- user
-- text
-- truncated
-- in_reply_to_screen_name
-- favorited
-- in_reply_to_user_id
-- created_at
-- source
-- in_reply_to_status_id
-- id


-- Tweet User
-- notifications
-- profile_text_color
-- profile_sidebar_fill_color
-- created_at
-- profile_background_tile
-- id
-- following
-- profile_image_url
-- url
-- screen_name
-- location
-- name
-- friends_count
-- utc_offset
-- verified_profile
-- statuses_count
-- time_zone
-- profile_background_image_url
-- favourites_count
-- profile_sidebar_border_color
-- followers_count
-- profile_link_color
-- description
-- profile_background_color
-- protected
