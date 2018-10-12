#!/usr/bin/env groovy
pipeline {
    agent any

    environment {
        DOCKER_LOCATION = './docker'
        COMPOSE_FILE = "${DOCKER_LOCATION}/docker-compose.yml"
        COMPOSE_PROJECT_NAME = "${GIT_COMMIT}_${BUILD_NUMBER}"
    }

    stages {
        stage('Build') {
            steps {
                echo 'Starting container'
                sh "docker-compose -f ${COMPOSE_FILE} up -d"
            }
        }
        stage('Prepare') {
            steps {
                echo 'Running go get'
                sh "docker exec -i ${COMPOSE_PROJECT_NAME}_go_1 bash -c 'go get -t ./...'"
            }
        }
        stage('Test') {
            steps {
                echo 'Running go tests'
                sh "docker exec -i ${COMPOSE_PROJECT_NAME}_go_1 bash -c 'go test ./...'"
            }
        }
    }

    post {
        always {
            echo "Removing docker container"
            sh "docker-compose down || true"
        }
    }
}