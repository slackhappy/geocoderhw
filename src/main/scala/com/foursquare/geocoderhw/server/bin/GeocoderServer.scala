package com.foursquare.geocoderhw.server.bin

import com.foursquare.geocoderhw.boot.Boot
import com.foursquare.geocoderhw.model.FeatureRecord
import com.google.common.geometry.{S2CellId, S2LatLng}
import com.codahale.jerkson.Json.{parse, stream}
import com.twitter.finagle.builder.{ServerBuilder, Server}
import com.twitter.finagle.http.{RichHttp, Http, Response, Request}
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.Service
import com.twitter.util.{Future, FuturePool, Throw}
import java.io.{InputStream, IOException}
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import org.jboss.netty.buffer.{ChannelBufferInputStream, ChannelBuffers}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import scalaj.collection.Imports._
import scala.collection.mutable.ListBuffer

class GeocoderHttpService(
    fileService: Service[Request, Response],
    apiService: Service[Request, Response]) extends Service[Request, Response] {

  def apply(request: Request) = {
    if (!request.path.startsWith("/api/")) {
      fileService(request)
    } else {
      apiService(request)
    }
  }
}


class ApiHttpService() extends Service[Request, Response] {

  val apiFuturePool = FuturePool(Executors.newFixedThreadPool(10))

  def apply(request: Request) = {
    Future(Response(request))
  }
}


class StaticFileService(prefix: String) extends Service[Request, Response] {

  val staticFileFuturePool = FuturePool(Executors.newFixedThreadPool(8))

  def inputStreamToByteArray(is: InputStream): Array[Byte] = {
    val buf = ListBuffer[Byte]()
    var b = is.read()
    while (b != -1) {
        buf.append(b.byteValue)
        b = is.read()
    }
    buf.toArray
  }


  def apply(request: Request) = {
    val path = if (request.path.startsWith("/content")) {
      request.path
    } else {
      "/content/index.html"
    }
    val resourcePath = prefix + path
    val stream = Option(getClass.getResourceAsStream(resourcePath))

    stream.map(s =>  staticFileFuturePool(inputStreamToByteArray(s)).map(data => {
      val response = Response(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        response.setContent(ChannelBuffers.copiedBuffer(data))
        if (path.endsWith(".js")) {
          response.headerMap.add(HttpHeaders.Names.CONTENT_TYPE, "application/x-javascript")
        }
        if (path.endsWith(".css")) {
          response.headerMap.add(HttpHeaders.Names.CONTENT_TYPE, "text/css")
        }
        response
    })).getOrElse(Future(Response(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)))
  }
}

object GeocoderServer {
  val DefaultPort = 8080

   def main(args: Array[String]) {
     Boot.bootMongo(Nil)

    val service = new GeocoderHttpService(
      new StaticFileService(""),
      new ApiHttpService()
    )

    val httpPort = Option(System.getProperty("server.port")).map(_.toInt).getOrElse(DefaultPort)
    val server: Server = ServerBuilder()
      .bindTo(new InetSocketAddress(httpPort))
      .codec(new RichHttp[Request](Http.get))
      .name("geocoder-http")
      .build(service)
   }
}

