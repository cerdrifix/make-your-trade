DO
$do$
begin
    if not exists ( select from pg_catalog.pg_user where usename = 'myt_user') then
        create user myt_user;

        alter user myt_user with encrypted password '$$myt_user$$';
    end if;

end;
$do$;

create database MYT;

grant all privileges on database MYT to myt_user;
