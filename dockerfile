FROM python:3.9
WORKDIR /src
COPY requirements.txt requirements.txt

ENV TZ=Asia/Almaty
RUN apt-get update && apt-get install -yy tzdata
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN pip install --no-cache-dir pymongo[srv]
RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "-u", "./main.py" ]
