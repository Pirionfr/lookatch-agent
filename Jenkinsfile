pipeline {
    agent {
        dockerfile {
            args '-u root:root'
        }
    }
    stages {
         stage('Init') {
            steps {
                withCredentials([file(credentialsId: '${gpgprivatekey}', variable: 'FILE')]) {
                    sh '''#!/bin/bash -xe
                        export GPG_TTY=$(tty)
                        gpg-connect-agent reloadagent /bye
                        gpg --batch --import $FILE
                    '''
                }
            }
        }
        stage('Build') {
            steps {
                sh '''
                    mkdir -p ${GOPATH}/src/github.com/Pirionfr/
                    ln -s ${WORKSPACE} ${GOPATH}/src/github.com/Pirionfr/lookatch-agent
                    cd ${GOPATH}/src/github.com/Pirionfr/lookatch-agent
                    make release
                '''
            }
        }
        stage("build artifacts") {
            steps {
                withCredentials([string(credentialsId: '${rpmpass}', variable: 'GPGTOKEN')]) {
                    sh '''#!/bin/bash -xe
                        make deb
                        make rpm
                        keygrip=$(gpg --with-keygrip -K C4F21B73 | grep Keygrip | head -1 | sed "s/ = /\\n/g" | tail -1)
                        gpg-preset-passphrase --passphrase ${GPGTOKEN} --preset ${keygrip}
                        rpm --resign -D "_signature gpg" -D "_gpg_name C4F21B73" lookatch-agent*.rpm
                    '''
                }
            }
        }
        stage("deploy artifacts") {
            steps {
                withCredentials([usernamePassword(credentialsId: '${repopassword}', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                    sh '''
                        filename="$(ls *.rpm | head -1)"
                        curl -u ${USERNAME}:${PASSWORD} --upload-file ${WORKSPACE}/${filename} ${repobaseurl}/centos/7/os/x86_64/${filename}

                        filename="$(ls *.deb | head -1)"
                        curl -u ${USERNAME}:${PASSWORD} -X POST -H "Content-Type: multipart/form-data" --data-binary "@${filename}" ${repobaseurl}/debian/
                    '''
                }
            }
        }
    }
}