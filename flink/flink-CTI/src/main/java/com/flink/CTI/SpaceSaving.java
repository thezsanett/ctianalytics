package com.flink.CTI;

import org.javatuples.Pair;

import java.io.Serializable;

public class SpaceSaving implements Serializable {
    int size;
    Pair<String, Integer>[] frequentItems;

    public SpaceSaving() {
    }


    public int getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }


    public void initSpaceSaving (int k){
        this.size =k;
        Pair<String,Integer>[] temp = new Pair[k];
        this.frequentItems = temp;
        System.out.println(this.frequentItems.length);
    }

    public void  updateSpaceSaving(String element){

        boolean controller = false;
        try{
        for(int i = 0;i< this.frequentItems.length ; i++) {
            if(this.frequentItems[i].getValue0() == element)
            {
                this.frequentItems[i].setAt1(this.frequentItems[i].getValue0()+1);
                controller = true;

            }}}
        catch (Exception e){
            System.out.println(e);
            System.out.println(controller);
        }
        if(controller!= true){

        for(int i = 0;i< this.frequentItems.length ; i++) {

            if(this.frequentItems[i]==null){

                this.frequentItems[i]= Pair.with(element,1) ;
                System.out.println(element);
               controller = true;
               break;
            }
        }
        }
            if (controller!= true){
                int minFrequentItemIndex=0 ;
                int minFrequentItemCount = this.frequentItems[minFrequentItemIndex].getValue1() ;
                for(int i = 0;i< this.frequentItems.length ; i++){
                    
                    if(minFrequentItemCount>=this.frequentItems[i].getValue1()){
                        minFrequentItemIndex = i;
                        minFrequentItemCount = this.frequentItems[i].getValue1();
                    }
                }
                this.frequentItems[minFrequentItemIndex].setAt0(element);
                this.frequentItems[minFrequentItemIndex].setAt1(minFrequentItemCount+1);

            }

    }

        public void query(){
        System.out.println("Heavy hitters are:");
        for(int i =0;i<this.frequentItems.length; i++){
            if(this.frequentItems[i]!=null){
            System.out.println(this.frequentItems[i].getValue0() + this.frequentItems[i].getValue1() + "<<<<<");
        }}

        }
}







