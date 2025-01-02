from metaflow import FlowSpec, step, Parameter
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import pickle

class MovieRecommendationFlow(FlowSpec):

    # Parameter to configure input data location
    data_path = Parameter('data_path', help="Path to sample movie data", default='../data/Sample_data.csv')
    
    @step
    def start(self):
        print("Starting the Movie Recommendation Flow...")
        self.next(self.ingest_data)

    @step
    def ingest_data(self):
        # Load sample data from CSV
        print(f"Loading sample data from {self.data_path}...")
        self.data = pd.read_csv(self.data_path, usecols=['user_id', 'movie_title', 'duration'])
        print(f"Data loaded successfully: \n{self.data.head()}")
        self.next(self.preprocess_data)

    @step
    def preprocess_data(self):
        # Preprocessing the data
        print("Preprocessing the data...")
        # Assuming duration is our target, mapping movie_title to numeric using LabelEncoder-like transformation
        self.data['movie_title_encoded'] = self.data['movie_title'].astype('category').cat.codes
        self.features = self.data[['user_id', 'movie_title_encoded']]
        self.labels = self.data['duration']
        print("Data preprocessed successfully.")
        self.next(self.train_model)

    @step
    def train_model(self):
        # Training a simple Random Forest model
        print("Training the recommendation model...")
        self.model = RandomForestClassifier(n_estimators=10)
        self.model.fit(self.features, self.labels)
        print("Model training complete.")
        # Save model for later use
        self.model_path = 'rf_model.pkl'
        with open(self.model_path, 'wb') as f:
            pickle.dump(self.model, f)
        print(f"Model saved at {self.model_path}")
        self.next(self.generate_recommendations)

    @step
    def generate_recommendations(self):
        # Generate a simple recommendation for a new user
        print("Generating recommendations...")
        # Mock data for a new user recommendation
        new_user = pd.DataFrame([[4, self.data['movie_title_encoded'].sample(1).values[0]]],
                                columns=['user_id', 'movie_title_encoded'])
        with open(self.model_path, 'rb') as f:
            loaded_model = pickle.load(f)
        prediction = loaded_model.predict(new_user)
        print(f"Recommendation duration for user 4 with a random movie: {prediction[0]} minutes")
        self.next(self.end)

    @step
    def end(self):
        print("Movie Recommendation Flow complete!")

if __name__ == '__main__':
    MovieRecommendationFlow()
