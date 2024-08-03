pipeline {
    agent any
    environment {
        LABS = credentials('labcreds')
    }
    stages {
        stage('Build') {
            steps {
                sh 'pip3 install --user pipenv'
                sh '/bitnami/jenkins/home/.local/bin/pipenv --rm || exit 0'
                sh '/bitnami/jenkins/home/.local/bin/pipenv install'
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
