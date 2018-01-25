package cn.wenbo.util

import java.util.Properties
import java.io.FileInputStream

object PropertyUtil {
  def loadProperties(key: String):String = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("data.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    properties.getProperty(key) //读取键为ddd的数据的值
   // println(properties.getProperty("ddd","没有值"))//如果ddd不存在,则返回第二个参数
    //properties.setProperty("ddd","123")//添加或修改属性值
  }
}
