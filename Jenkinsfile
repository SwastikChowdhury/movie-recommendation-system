pipeline {
    agent any
    environment {
        API_KEY = credentials('TMDB_API_KEY')
        DB_PASSWORD = credentials('DB_PASSWORD')
        DB_NAME = credentials('DB_NAME')
        DB_USER = credentials('DB_USER')
    }
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        stage('Install Dependencies') {
            steps {
                sh '''
                python3 -m venv venv
                . venv/bin/activate
                pip install -r requirements.txt
                '''
            }
        }
        stage('Run Tests') {
            steps {
                sh '''
                . venv/bin/activate
                pytest --junitxml=tests/test-results.xml tests/
                coverage run -m pytest --junitxml=tests/test-results.xml tests/
                coverage report
                coverage html
                '''
            }
        }
    }
    post {
        always {
            junit 'tests/test-results.xml'
        }
        failure {
            echo 'Some tests failed.'
        }
    }
}
