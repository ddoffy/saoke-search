# The Python script extracts the text from the PDF file and expose it as an API using FastAPI that allows you to search for the text in the PDF file.

### Here's the [PDF](https://drive.google.com/file/d/18dIWiReYtJkyuQ_8vSBJWweGaD71rBpu/view?fbclid=IwY2xjawFRjZNleHRuA2FlbQIxMAABHXL3xxUyBDnJq6Eo8o5Xv0uEDxijDoaNfsTP60w3wFSJhPsGICy--gRNyQ_aem_ZSdVmI7AIXVLKRWzTJZ26A&pli=1) file that I used in this script or you can find the PDF file in the root of the project.

### [Thong Tin Chinh Phu](https://www.facebook.com/thongtinchinhphu/posts/pfbid03YkRTKZ5WjeHwBavPQbP7EShonj9tTExgY26gNhvQdiEsbjdsLWnzWEoQE1bU9SBl) shared the PDF file mentioned above on Facebook.

# Run on your local machine
### Install python 3

### Install python libraries using the following command at the root of the project:
```
pip install -r requirements.txt
```

### Run the script using the following command:
##### if you wanna run the script in development mode
```
fastapi dev main.py 
```
##### if you wanna run the script in production mode
```
fastapi run
```

# Run with Docker 
### Build the Docker image using the following command:
```
docker build -t mttq-saoke .
```

### Run the Docker container using the following command:
```
docker run -d -p 8000:8000 mttq-saoke
```

### Open the browser and go to the following URL:
```
http://localhost:8000/docs
```

### You can see the Swagger UI and test the API.

# Using curl to test the API
### Search for the text in the PDF file using the following command:
```
curl -X 'GET' \
  'http://localhost:8000/?q=[your value with url encode]' \
  -H 'accept: application/json'
```

# Try it directly on the browser
### Open the browser and go to the following URL:
- [UI for searching](https://saokeui.doffylaw.org)
- [Docs](https://saokeapi.doffylaw.org/docs)
