package mrkprototypes.kprototypes;

import org.apache.hadoop.conf.Configuration;
public class TestReadEnv {


public static void main(String[] args) {
Configuration conf = new Configuration();
conf.addResource("conf/kprototypesSetting.xml");
System.out.println(conf.getDouble("epsilon", 0.0));

}


}