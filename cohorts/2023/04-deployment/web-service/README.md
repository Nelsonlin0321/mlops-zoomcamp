# Web Service

### Using Pipenv install environment

```bash
pipenv install scikit-learn==1.0.2 flask --python 3.9
pipenv shell
PS1="> "
```

```bash
pipenv install gunicorn 
```

```bash
gunicoren--bind=0.0.0.0:9696 predict:app
```

```bash
pipenv install --dev requests
```