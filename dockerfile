# Use an official Python runtime as a parent image
FROM python:3.9

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set the working directory to /app
WORKDIR /usr/app

# Copy the application files to the container
COPY .env service_account.json enrolling_sites.csv gather_info.py send_results_email.py send_email.py /usr/app/

# make referenced folders
RUN mkdir /usr/app/logs

# set and expose port
ENV PORT 8080
EXPOSE 8080

# Start selenium server and run the Selenium script
CMD ["python", "send_email.py"]