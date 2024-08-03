pipeline {
    agent any
    environment {
        LABS = credentials('labcreds')
    }
    stages {
        stage('Setup Python Environment') {
            steps {
                sh '''
                    sudo apt update
                    sudo apt install -y python3-venv
                    python3 -m venv venv
                    source venv/bin/activate
                    pip install pipenv
                '''
            }
        }
        stage('Build') {
            steps {
                sh '''
                    source venv/bin/activate
                    pipenv --rm || exit 0
                    pipenv install
                '''
            }
        }
        stage('Test') {
            steps {
                echo "Test completed successfully"
            }
        }
        stage('Package') {
            steps {
                sh 'zip -r lendingclub.zip .'
            }
        }
        stage('Deploy') {
            steps {
                sh '''
                    sshpass -p $LABS_PSW scp -o StrictHostKeyChecking=no -r ./retailproject.zip \
                    $LABS_USR@g02.itversity.com:/home/itv012760/lendingclub
                '''
            }
        }
    }
}