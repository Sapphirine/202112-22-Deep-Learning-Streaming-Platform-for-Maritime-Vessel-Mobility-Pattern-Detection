This folder contains three notebooks:
<li>
  <b>eecs6893_visualize_vessel_image_data.ipynb</b> (this notebook downloads training, validation and test from S3 buckets, prints information about different classes and visualizes images for data exploration)
<li>
  <b>eecs6893_ais_model.ipynb</b> (this notebook trains four different Deep Learning models and gathers validation and test metrics. It has evaulation plots and also has code fo batch prediction on test data)
 <li>
   <b>eecs6893_sagemake_js_endpoint.ipynb</b> (this notebook contains code for calling SageMaker model endpoint for use in any Streaming mode for example from Airflow)
   
   
Deep Learninig Models we evaluated:
   <li>Simple CNN consisting of 3 layers
   <li>Inception V3 pre-trained model for transfer learning by training on our image data
   <li>MobileNet V2 pre-trained model for transfer learning by training on our image data
   <li>EfficientNetB0 pre-trained model for transfer learning by training on our image data
