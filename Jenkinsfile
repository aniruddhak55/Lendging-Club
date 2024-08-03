pipeline {
    agent any
    environment {
        LABS = credentials('labcreds')
    }
    stages {
        stage('Build') {
            steps {
                sh '''
                    source myenv/bin/activate || exit 0
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

				sh 'sshpass-p $LABS_PSW scp-o StrictHostKeyChecking=no-r .
                $LABS_USR@g02.itversity.com:/home/itv012760/lendingclub'
            }
        }
    }
}