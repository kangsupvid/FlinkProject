package bean

class Student () extends Serializable{

  private var fields2 = new Array[Any](0)


  /**
    * Creates an instance of RowSerializable
    *
    * @param i Size of the row
    */

  def setFields2(i: Int) = {
    fields2 = new Array[Any](i)
  }
  /**
    * returns number of fields contained in a Row
    *
    * @return int arity
    */
  def productArity: Int = this.fields2.length

  /**
    * Inserts the "field" Object in the position "i".
    *
    * @param i     index value
    * @param field Object to write
    */
  def setField(i: Int, field: Any) = {
    this.fields2(i) = field
  }


  /**
    * returns the Object contained in the position "i" from the RowSerializable.
    *
    * @param i index value
    * @return Object
    */
  def productElement(i: Int): Any = this.fields2(i)


  override def equals(`object`: Any) = false
}

