import groovy.json.JsonSlurper
// This Jenkinsfile is used by Jenkins to run the graph-importer step of Reactome's release.
// It requires that OrthoInferenaceStableIDHistory has run successfully
def currentRelease
def previousRelease
pipeline
{
	agent any
	environment {
	       ECRURL = '851227637779.dkr.ecr.us-east-1.amazonaws.com'
	    }
	    
	    stages {
	        stage('pull image') {
	            steps {
	                script{
	                    sh("eval \$(aws ecr get-login --no-include-email --region us-east-1)")
	                    docker.withRegistry("https://" + ECRURL) {
	                        docker.image("graph-importer:latest").pull()
	                    }
	                }
	            }
	        }
		stage('Setup: Rename database')
		{
			// Normally, the relational database is named "release_current" but it need to be renamed to "reactome" because that's the name
			// that needs to be in the graph database, otherwise the AnalysisService will report an error.
			// "Renaming" the database really involves dumping to a temp file and then restoring it with a different name.
			// TODO: Finish this stage.
			steps
			{
				script
				{
					withCredentials([usernamePassword(credentialsId: 'mySQLUsernamePassword', passwordVariable: 'pass', usernameVariable: 'user')])
					{
						def dumpfile = "${env.RELEASE_CURRENT}_${currentRelease}_before_graph_import.dump"
						sh "mysqldump -u$user -p$pass ${env.RELEASE_CURRENT} > $dumpfile"
						sh "mysql -h localhost -u$user -p$pass reactome < $dumpfile "
					}
				}
			}
		}
		stage('Main: Run graph-importer')
		{
			steps
			{
				script
				{
					withCredentials([usernamePassword(credentialsId: 'mySQLUsernamePassword', passwordVariable: 'pass', usernameVariable: 'user')])
					{
						sh """docker run -v \$(pwd)/output:/graphdb --net=host  ${ECRURL}/graph-importer:latest /bin/bash -c java -jar target/GraphImporter-exec.jar -h localhost -i -n ./graphdb -d reactome -u $user -p $pass """
					}
				}
			}
		}
		stage('Post: Install graph database')
		{
			steps
			{
				script
				{
					// Stop services that are using the graph database.
					sh "service tomcat9 stop"
					sh "service neo4j stop"
				}
				script
				{
					// Remove old graphdb
					sh "rm -rf /var/lib/neo4j/data/databases/graph.db"
					// Set permissions
					sh "chmod 644 ./graph.db/*"
					sh "chmod a+x ./graph.db/schema/"
					sh "chown -R neo4j:adm ./graph.db/"
					// Move the graph database into position.
					sh "mv -a ./graphdb /var/lib/neo4j/data/databases/graph.db"
				}
				script
				{
					// Start the services that were stopped.
					sh "service neo4j start"
					sh "service tomcat9 start"
				}
			}
		}
		// TODO: Maybe use the actual "post" directive?
		stage('Post: Archive logs, run qa')
		{
			steps
			{
				script
				{
					sh "mkdir -p archive/${currentRelease}/logs"
					sh "mv logs/* archive/${currentRelease}/logs"
				}
			}
			stages
			{
				stage('graph-qa')
				{
					steps
					{
						checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: '*/master']], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github credentials', url: 'https://github.com/reactome/graph-qa.git']]]
						dir('./graph-importer')
						{
							script
							{
								sh "mvn clean package -DskipTests"
							}
							withCredentials([usernamePassword(credentialsId: 'graphdbCredentials', passwordVariable: 'graphdbPassword', usernameVariable: 'graphdbUser')])
							{
								script
								{
									def qaSummary = sh(returnStdout: true, script: "java -jar target/graph-qa-exec.jar -h localhost -u $graphdbUser -p $graphdbPassword -o ./$currentRelease -v").trim()
									sh "tar -czf ./$currentRelease.tgz ./$currentRelease"
									emailext attachmentsPattern: "$currentRelease.tgz", body: '''Graph-qa has finished. Summary is:

$qaSummary

Detailed reports are attached.''', subject: "graph-qa results", to: "reactome-developer@reactome.org"
								}
							}
						}
					}
				}
			}
		}
	}
}

// Utility function that checks upstream builds of this project were successfully built.
def checkUpstreamBuildsSucceeded(String stepName, String currentRelease)
{
	def statusUrl = httpRequest authentication: 'jenkinsKey', validResponseCodes: "${env.VALID_RESPONSE_CODES}", url: "${env.JENKINS_JOB_URL}/job/$currentRelease/job/$stepName/lastBuild/api/json"
	if (statusUrl.getStatus() == 404)
	{
		error("$stepName has not yet been run. Please complete a successful build.")
	}
	else
	{
		def statusJson = new JsonSlurper().parseText(statusUrl.getContent())
		if(statusJson['result'] != "SUCCESS")
		{
			error("Most recent $stepName build status: " + statusJson['result'] + ". Please complete a successful build.")
		}
	}
}
