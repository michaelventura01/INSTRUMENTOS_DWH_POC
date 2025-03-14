# Use the official Python 3.8 slim image
FROM python:3.8-slim

# Set the working directory
WORKDIR /opt/carga

# Copy the requirements file and install dependencies
COPY requirements.txt /opt/carga/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the files from the carga directory
COPY . /opt/carga

# Set the default command to run your Python script
CMD ["python", "/opt/carga/database_operation.py"]