FROM python:3.12-slim-bullseye
WORKDIR /app


# Install git
RUN apt-get update && apt-get install -y git

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY . /app

EXPOSE 4000


# Define environment variables
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0

CMD [ "flask", "run", "--host=0.0.0.0", "--port=4000" ]