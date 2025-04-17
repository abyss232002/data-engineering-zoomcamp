FROM python:3.12.7

RUN pip install pandas
WORKDIR /app
COPY ["./pipeline.py", "pipeline.py"]

# ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "pipeline.py" ]