package pub.ayada.scala.hdfsutils

import scala.collection.JavaConverters._
import scala.collection.mutable.{ ListBuffer }
import org.apache.hadoop.security.alias.JavaKeyStoreProvider

class KeyStore {

}
object KeyStore {

    def getPassword(conf: org.apache.hadoop.conf.Configuration, alias: String): String = {
        new String(conf.getPassword(alias));
    }

    def getAllAliasNPasswords(conf: org.apache.hadoop.conf.Configuration): List[(String, String)] = {
        var lst: ListBuffer[(String, String)] = ListBuffer[(String, String)]()
        for (alias: String <- listAliassesFromAllProviders(conf)) {
            lst += (alias -> getPassword(conf, alias))
        }
        lst.toList
    }

    def listAliassesFromAllProviders(conf: org.apache.hadoop.conf.Configuration): List[String] = {
        val providers: java.util.List[org.apache.hadoop.security.alias.CredentialProvider] =
            org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders(conf)

        var alias: ListBuffer[String] = ListBuffer[String]()
        val it = providers.iterator
        while (it.hasNext) {
            for (pr <- it.next.getAliases.asScala) { alias += pr }
        }
        alias.toList
    }

    def getCredentialProviderURI(conf: org.apache.hadoop.conf.Configuration): List[String] = {
        var alias: ListBuffer[String] = ListBuffer[String]()
        for (s: String <- conf.getStringCollection("hadoop.security.credential.provider.path").asScala) {
            alias += s
        }
        alias.toList
    }

    def deleteAliasFromAllProviders(conf: org.apache.hadoop.conf.Configuration, alias: String): Unit = {
        val providers: java.util.List[org.apache.hadoop.security.alias.CredentialProvider] =
            org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders(conf)
        println("\n\nKeyStore.deleteAliasFromAllProviders(): Adding alias to the providers:" + alias)
        val it = providers.iterator
        var i: Int = 1
        while (it.hasNext) {
            println("\t\t" + i + "Deleting")
            val provider: org.apache.hadoop.security.alias.CredentialProvider = it.next;
            provider.deleteCredentialEntry(alias)
            provider.flush()
        }
    }

    def createAliasInAllProviders(conf: org.apache.hadoop.conf.Configuration, alias: String, credential: Array[Char]): Unit = {
        val providers: java.util.List[org.apache.hadoop.security.alias.CredentialProvider] =
            org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders(conf)

        val it = providers.iterator
        var i: Int = 1
        println("\n\nKeyStore.createAliasInAllProviders(): Adding alias to the providers:" + alias)
        while (it.hasNext) {
            println("\t\t" + i + "Adding")
            val provider: org.apache.hadoop.security.alias.CredentialProvider = it.next;
            provider.createCredentialEntry(alias, credential)
            provider.flush()
            i += 1
        }

    }
}
