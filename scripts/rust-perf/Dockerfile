FROM python:3.13.7-slim-bookworm@sha256:adafcc17694d715c905b4c7bebd96907a1fd5cf183395f0ebc4d3428bd22d92d
WORKDIR /fixture
COPY --chmod=0444 fixture.py /fixture/fixture.py
USER 65534:65534
EXPOSE 9292
HEALTHCHECK --interval=2s --timeout=2s --retries=15 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9292/metrics', timeout=1).read()"
CMD ["python", "-u", "/fixture/fixture.py"]
