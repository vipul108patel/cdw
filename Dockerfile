FROM python:3.9

WORKDIR / cdw

COPY .. .

#COPY . /
#ADD . /test2

RUN pip install -r requirements.txt

EXPOSE 8004

ENTRYPOINT ["python"]

CMD ["main.py"]

#
#FROM python:3.9
#
##
#WORKDIR /code
#
##
#COPY ./requirements.txt /code/requirements.txt
#
##
#RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
#
##
#COPY ./app_news.py /code /
#
##
#CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "80"]