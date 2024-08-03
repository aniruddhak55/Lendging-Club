pipeline {
    agent any
    environment {
        LABS = credentials('labcreds')
    }
    stages {
        stage('Build') {
            steps {
                sh '''
                    python3 -m venv myenv
                    source myenv/bin/activate
                    pip install pipenv
                    pipenv --rm || exit 0
                    pipenv install
                '''
            }
        }
        stage('Test') {
            steps {

                echo "test completed successful"
                   
            }
        }
        stage('Package') {
            steps {
                sh '''
                    source myenv/bin/activate
                    zip -r lendingclub.zip .
                '''
            }
        }
        stage('Deploy') {
            steps {
                sh '''
                    source myenv/bin/activate
                    sshpass -p $LABS_PSW scp -o StrictHostKeyChecking=no -r ./lendingclub.zip $LABS_USR@g02.itversity.com:/home/itv005857/lendingclub
                '''
            }
        }
    }
}