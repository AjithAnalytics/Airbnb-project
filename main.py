import pandas as pd
import numpy as np
import datetime as dt
from pymongo.mongo_client import MongoClient

class AirbnbDataProcessor:
    def __init__(self, mongodb_uri):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['sample_airbnb']
        self.collection = self.db['listingsAndReviews']

    def extract_data_from_mongodb(self):
        rel_data = []
        for i in self.collection.find():
            data = dict( Id = i['_id'],
                Listing_url = i['listing_url'],
                Name = i.get('name'),
                space = i['space'],
                Description = i['description'],
                neighborhood_overview = i['neighborhood_overview'],
                notes = i['notes'],
                transit = i['transit'],
                access = i['access'],
                interaction = i['interaction'],
                House_rules = i.get('house_rules'),
                Property_type = i['property_type'],
                Room_type = i['room_type'],
                Bed_type = i['bed_type'],
                Minimum_nights = i['minimum_nights'],
                Maximum_nights = i['maximum_nights'],
                Cancellation_policy = i['cancellation_policy'],
                last_scraped = i['last_scraped'],
                calendar_last_scraped = i[ 'calendar_last_scraped'],
                first_review = i.get('first_review'),
                last_review = i.get('last_review'),
                Accomodates = i['accommodates'],
                bedrooms = i.get('bedrooms'),
                beds = i.get('beds'),
                number_of_reviews = i['number_of_reviews'],
                bathrooms = i.get('bathrooms'),
                Amenities = ', '.join(i['amenities']),
                Price = i['price'],
                Extra_people = i['extra_people'],
                Guests_included= i['guests_included'],
                images_picture_url = i['images']['picture_url'],
                host_id = i['host']['host_id'],
                host_url = i['host']['host_url'],
                host_name = i['host']['host_name'],
                host_location =i['host']['host_location'],
                host_about = i['host']['host_about'],
                host_response_time = i.get('host', {}).get('host_response_time'),
                host_thumbnail_url = i['host']['host_thumbnail_url'],
                host_picture_url = i['host']['host_picture_url'],
                host_neighbourhood = i['host']['host_neighbourhood'],
                host_response_rate = i.get('host', {}).get('host_response_rate'),
                host_is_superhost = i['host']['host_is_superhost'],
                host_has_profile_pic = i['host']['host_has_profile_pic'],
                host_identity_verified = i['host']['host_identity_verified'],
                host_listings_count = i['host']['host_listings_count'],
                host_total_listings_count = i['host']['host_total_listings_count'],
                host_verifications = i['host']['host_verifications'],
                Street = i['address']['street'],
                Suburb = i['address']['suburb'],
                Government_Area = i['address']['government_area'],
                Market = i['address']['market'],
                Country = i['address']['country'],
                Country_Code = i['address']['country_code'],
                Location_type = i['address']['location']['type'],
                Longitude = i['address']['location']['coordinates'][0],
                Latitude = i['address']['location']['coordinates'][1],
                Is_Location_Exact = i['address']['location']['is_location_exact'],
                review_scores_accuracy = i['review_scores'].get('review_scores_accuracy'),
                review_scores_cleanliness = i['review_scores'].get('review_scores_cleanliness'),
                review_scores_checkin = i['review_scores'].get('review_scores_checkin'),
                review_scores_communication = i['review_scores'].get('review_scores_communication'),
                review_scores_location = i['review_scores'].get('review_scores_location'),
                review_scores_value = i['review_scores'].get('review_scores_value'),
                review_scores_rating = i['review_scores'].get('review_scores_rating'),
                availability_30 = i['availability'].get('availability_30'),
                availability_60 = i['availability'].get('availability_60'),
                availability_90= i['availability'].get('availability_90'),
                availability_365= i['availability'].get('availability_365')
            )
            rel_data.append(data)
        return pd.DataFrame(rel_data)
    
    # Handle missing data as needed
    def handle_missing_data(self, data):
        data['beds'].fillna(0,  axis=0, inplace=True)
        data['bedrooms'].fillna(0,  axis=0, inplace=True)
        data['bathrooms'].fillna(0,  axis=0, inplace=True)

        return data
    
      # Convert data types
    def convert_data_types(self, data):
        # integer
        data['Id'] = data['Id'].astype(int)
        data['host_id'] = data['host_id'].astype(int)
        data['Minimum_nights'] = data['Minimum_nights'].astype(int)
        data['Maximum_nights'] = data['Maximum_nights'].astype(int)
        data['Accomodates'] = data['Accomodates'].astype(int)
        data['bedrooms'] = data['bedrooms'].astype(int)
        data['beds'] = data['beds'].astype(int)
        data['number_of_reviews'] = data['number_of_reviews'].astype(int)

        #float
        data['bathrooms'] = data['bathrooms'].astype(str).astype(float)
        data[ 'Price'] = data[ 'Price'].astype(str).astype(float)
        data[ 'Extra_people'] = data[ 'Extra_people'].astype(str).astype(float)
        data[ 'Guests_included'] = data[ 'Guests_included'].astype(str).astype(float)

        # datetime
        data['last_scraped'] = pd.to_datetime(data['last_scraped'], format = '%Y-%m-%d').dt.date
        data['calendar_last_scraped'] = pd.to_datetime(data['calendar_last_scraped'], format = '%Y-%m-%d').dt.date
        #data['first_review'] = pd.to_datetime(data['first_review'], format = '%Y-%m-%d').dt.date
        data['first_review'] = pd.to_datetime(data['first_review'], errors='coerce', format='%Y-%m-%d')
        default_date = pd.to_datetime('1970-01-01')
        data['first_review'].fillna(default_date, inplace=True)
        data['first_review'] = data['first_review'].dt.date
        #data['last_review'] = pd.to_datetime(data['last_review'], format = '%Y-%m-%d').dt.date
        data['last_review'] = pd.to_datetime(data['last_review'], errors='coerce', format='%Y-%m-%d')
        default_date = pd.to_datetime('1970-01-01')
        data['last_review'].fillna(default_date, inplace=True)
        data['last_review'] = data['last_review'].dt.date
        
        return data
    
    def preprocess_data(self, data):
        duplicates = data.duplicated(subset = 'Id', keep = 'first')
        if not duplicates.empty:
            data.drop(data[duplicates].index, inplace = True)

        inconsistent_days=data[(data['availability_30'] < 0 ) & (data['availability_30'] >30)]
        if not inconsistent_days.empty:
            data.drop(inconsistent_days.index, inplace = True)

        inconsistent_days=data[(data['availability_60'] <0 ) & (data['availability_60'] >60)]
        if not inconsistent_days.empty:
            data.drop(inconsistent_days.index, inplace = True)

        inconsistent_days=data[(data['availability_90'] <0 ) & (data['availability_90'] >90)]
        if not inconsistent_days.empty:
            data.drop(inconsistent_days.index, inplace = True)

        inconsistent_days=data[(data['availability_365'] <0 ) & (data['availability_365'] >365)]
        if not inconsistent_days.empty:
            data.drop(inconsistent_days.index, inplace = True)

        inconsistent_dates = data[(data['last_review'] > dt.date.today()) | (data['first_review']> dt.date.today())]
        if not inconsistent_dates.empty:
            data.drop(inconsistent_dates.index, inplace = True)

        inconsistent_dates = data[data['first_review'] > data['last_review']]
        if not inconsistent_dates.empty:
            data.drop(inconsistent_dates.index, inplace = True)

        is_host_response = np.where((data['host_response_time'].isna() == True) & (data['host_response_rate'].isna() == True), 0, 1)
        if len(is_host_response):
            data['is_host_response'] = is_host_response

            data = data.fillna({'host_response_time':0,
                                'host_response_rate':0})

        is_review_scores = np.where((data['review_scores_accuracy'].isna() == True) & (data['review_scores_cleanliness'].isna() == True) &
                                (data['review_scores_checkin'].isna() == True) & (data['review_scores_communication'].isna() == True) &
                                (data['review_scores_location'].isna() == True) & (data['review_scores_value'].isna() == True) &
                                (data['review_scores_rating'].isna() == True)
                                , 0, 1)

        if len(is_review_scores):
            data['is_review_scores'] = is_review_scores

            data= data.fillna({'review_scores_accuracy':0,
                                'review_scores_cleanliness':0,
                                'review_scores_checkin':0,
                                'review_scores_communication':0,
                                'review_scores_location':0,
                                'review_scores_value':0,
                                'review_scores_rating':0,})
        return data
    
    def process_airbnb_data(self):
        raw_data = self.extract_data_from_mongodb()
        data = self.handle_missing_data(raw_data)
        data1 = self.convert_data_types(data)
        processed_data = self.preprocess_data(data1)
        return processed_data
    
if __name__ == "__main__":
    # Update the MongoDB URI with your actual connection string
    airbnb_processor = AirbnbDataProcessor("mongodb+srv://farmerajith:Sandy@cluster0.xb14cgk.mongodb.net/?retryWrites=true&w=majority")
    processed_data = airbnb_processor.process_airbnb_data()
    
processed_data=pd.DataFrame(processed_data)
# Save DataFrame to CSV file
file_path = "Airbnbdemodata.xlsx"
processed_data.to_excel(file_path, index=False)
print(f"excel file saved at: {file_path}")
