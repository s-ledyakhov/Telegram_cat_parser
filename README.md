# Telegram_cat_parser

The Python bot, which will collect the cats for you, will send it to the moderator's chat to check the quality of the cats, then it will post to the channel in the time range specified by Cron.

You should specify your telegram_api data, api_key, database credentials in the config file.

I also prepared a dockerfile for you, just put all the files in one directory on your docker station and do:
docker build -t poster .
docker run -d --name cat_poster poster 

If you want to do a CDI/CD for your bot, I'll also attach a simple file for the gitlab runner

(In fact, you can parse all sorts of things, but for a simple example, I chose kittens. Everyone loves kittens 🐈)
