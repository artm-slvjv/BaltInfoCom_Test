package org.example;

public class Pair {
    
    private Integer foundGroup;
    private Integer firstValueIndex;
    private Integer secondValueIndex;

    public Pair() {
        this.foundGroup = null;
        this.firstValueIndex = null;
        this.secondValueIndex = null;
    }

    public void setFoundGroup(Integer foundGroup) {
        this.foundGroup = foundGroup;
    }

    public void setFirstValueIndex(Integer firstValueIndex) {
        this.firstValueIndex = firstValueIndex;
    }

    public void setSecondValueIndex(Integer secondValueIndex) {
        this.secondValueIndex = secondValueIndex;
    }

    public Integer getFoundGroup() {
        return foundGroup;
    }

    public Integer getFirstValueIndex() {
        return firstValueIndex;
    }

    public Integer getSecondValueIndex() {
        return secondValueIndex;
    }

    public boolean isEmpty(){
        return (this.foundGroup == null & this.firstValueIndex == null & this.secondValueIndex == null);
    }

}
