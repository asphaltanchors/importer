FROM meltano/meltano:latest

RUN apt-get update && apt-get install -y cron

WORKDIR /project

# Install any additional requirements
COPY ./requirements.txt .
RUN pip install -r requirements.txt

# Copy over Meltano project directory
COPY . .
RUN meltano install

# Don't allow changes to containerized project files
ENV MELTANO_PROJECT_READONLY=1

# Copy your crontab into /etc/cron.d
COPY cronjob /etc/cron.d/pipeline-cron

# Give it the right permissions so cron will read it
RUN chmod 0644 /etc/cron.d/pipeline-cron \
    # (optional) install it to root's crontab, so you don't need the 'user' field in cronjob
    && crontab /etc/cron.d/pipeline-cron 

CMD ["cron", "-f"]
