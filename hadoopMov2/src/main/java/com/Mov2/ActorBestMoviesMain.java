package com.Mov2;

import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;


public class ActorBestMoviesMain {
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("O:/Film.json"));
        FileWriter fw = new FileWriter(new File("Film.csv"));
//MovieInfo 类可以参考实验1
        MovieInfo m = null;
        String line = null;
        while ((line = br.readLine()) != null) {
            //Fastjson 把每行的json 字符串转换为对象。
            //m="title":"布达佩斯大饭店 The Grand Budapest Hotel","year":"2014","type":"剧情,喜剧,冒险","star":8.8,"director":"韦斯·安德森","actor":"拉尔夫·费因斯,托尼·雷沃罗利,艾德里安·布洛迪,威廉·达福,裘德·洛,爱德华·诺顿,西尔莎·罗南,蒂尔达·斯文顿,比尔·默瑞,蕾雅·赛杜,欧文·威尔逊,詹森·舒瓦兹曼,马修·阿马立克,F·默里·亚伯拉罕,汤姆·威尔金森,杰夫·高布伦,哈威·凯特尔","pp":343277,"time":99,"film_page":"https://movie.douban.com/subject/11525673/"}
            m = JSON.parseObject(line, MovieInfo.class);

            if (m.getActor().indexOf("威廉·达福") != -1) {
                //Film_page 作为电影ID,"film_page":"https://movie.douban.com/subject/11525673/"
                String mId = m.getFilm_page();
                //取出演员的列表,"actor":"拉尔夫·费因斯,托尼·雷沃罗利,艾德里安·布洛迪,威廉·达福,裘德·洛,爱德华·诺顿,西尔莎·罗南,蒂尔达·斯文顿,比尔·默瑞,蕾雅·赛杜,欧文·威尔逊,詹森·舒瓦兹曼,马修·阿马立克,F·默里·亚伯拉罕,汤姆·威尔金森,杰夫·高布伦,哈威·凯特尔"
                String[] actors = m.getActor().split(",");

                for (String ac : actors) {
                    //把电影数据写入csv文件。csv 表头为 ID,电影名称,评分,演员
                    fw.append(mId + "#" + m.getTitle() + "#" + ac + "#" + m.getStar() + "\n");
                    fw.flush()
                }
            }
        }


    }
}
