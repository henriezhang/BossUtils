package com.webdev.boss;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by henriezhang on 2014/12/9.
 */
public class Act3dEuclideanDistance extends UDF{
    private double norm(double v, double max) {
        double result = v / max;
        if(result>1.0) {
            result = 1.0;
        }
        return result;
    }

    public Double evaluate(Double days_act, Double site_act, Double pv_act) {
        double d_act = norm(days_act, 30);
        double s_act = norm(site_act, 10);
        double p_act = norm(pv_act, 30);
        return Math.sqrt((d_act*d_act) + (s_act*s_act) + (p_act*p_act));
    }
}
